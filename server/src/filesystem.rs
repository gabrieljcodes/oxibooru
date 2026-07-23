use crate::api::error::{ApiError, ApiResult};
use crate::config::Config;
use crate::content::hash::PostHash;
use crate::content::thumbnail::ThumbnailCategory;
use crate::content::upload::UploadToken;
use crate::model::enums::MimeType;
use axum::body::Bytes;
use futures::StreamExt;
use image::error::ImageError;
use image::{DynamicImage, ImageResult};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use strum::{Display, IntoStaticStr};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::MissedTickBehavior;
use tracing::warn;
use opendal::Operator;

/// Represents important data directories.
#[derive(Clone, Copy, Display, IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum Directory {
    Avatars,
    Posts,
    GeneratedThumbnails,
    CustomThumbnails,
    TemporaryUploads,
}

/// Returns the size of the file at `path` in bytes as an i64
pub fn file_size(path: &Path) -> std::io::Result<i64> {
    path.metadata()
        .map(|metadata| i64::try_from(metadata.len()).expect("File size must be less than i64::MAX"))
}

/// Saves streamed file contents to the temporary uploads folder as a `mime_type` file.
/// Returns the name of the file written.
///
/// Does not perform cleanup on error. It instead relies on the cleanup task spawned from
/// `spawn_temporary_uploads_cleanup_task` to clean out failed uploads.
pub async fn save_uploaded_file<S, E>(config: &Config, mut stream: S, mime_type: MimeType) -> ApiResult<UploadToken>
where
    S: StreamExt<Item = Result<Bytes, E>> + Unpin,
    ApiError: From<E>,
{
    std::fs::create_dir_all(config.path(Directory::TemporaryUploads))?;

    let upload_token = UploadToken::new(mime_type);
    let upload_path = upload_token.path(config);

    let mut file = File::create(upload_path).await?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    Ok(upload_token)
}

/// Saves custom avatar `thumbnail` for user with name `username` to disk/S3.
/// Returns size of the thumbnail in bytes.
pub async fn save_custom_avatar(config: &Config, operator: &Operator, lowercase_username: &str, thumbnail: DynamicImage) -> ApiResult<i64> {
    let avatar_key = config.custom_avatar_key(lowercase_username);
    let mut buf = std::io::Cursor::new(Vec::new());
    thumbnail.into_rgb8().write_to(&mut buf, image::ImageFormat::Png).map_err(ImageError::from)?;
    let data = buf.into_inner();
    let size = data.len() as i64;
    operator.write(&avatar_key, data).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    Ok(size)
}

/// Deletes custom avatar for user with name `username` from disk/S3, if it exists.
pub async fn delete_custom_avatar(config: &Config, operator: &Operator, lowercase_username: &str) -> ApiResult<()> {
    let custom_avatar_key = config.custom_avatar_key(lowercase_username);
    remove_if_exists_async(operator, &custom_avatar_key).await?;
    Ok(())
}

/// Saves `post` `thumbnail` to disk/S3. Can be custom or automatically generated.
/// Returns size of the thumbnail in bytes.
pub async fn save_post_thumbnail(
    post: &PostHash<'_>,
    operator: &Operator,
    thumbnail: DynamicImage,
    thumbnail_type: ThumbnailCategory,
) -> ApiResult<i64> {
    let thumbnail_key = match thumbnail_type {
        ThumbnailCategory::Generated => post.generated_thumbnail_key(),
        ThumbnailCategory::Custom => post.custom_thumbnail_key(),
    };
    let mut buf = std::io::Cursor::new(Vec::new());
    thumbnail.into_rgb8().write_to(&mut buf, image::ImageFormat::Jpeg).map_err(ImageError::from)?;
    let data = buf.into_inner();
    let size = data.len() as i64;
    operator.write(&thumbnail_key, data).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    Ok(size)
}

/// Deletes thumbnail of `post` from disk/S3, if it exists.
pub async fn delete_post_thumbnail(post: &PostHash<'_>, operator: &Operator, thumbnail_type: ThumbnailCategory) -> std::io::Result<()> {
    let thumbnail_key = match thumbnail_type {
        ThumbnailCategory::Generated => post.generated_thumbnail_key(),
        ThumbnailCategory::Custom => post.custom_thumbnail_key(),
    };
    remove_if_exists_async(operator, &thumbnail_key).await
}

/// Deletes `post` content from disk/S3.
pub async fn delete_content(post: &PostHash<'_>, operator: &Operator, mime_type: MimeType) -> std::io::Result<()> {
    let content_key = post.content_key(mime_type);
    remove_if_exists_async(operator, &content_key).await
}

/// Deletes `post` thumbnails and content from disk/S3.
pub async fn delete_post(post: &PostHash<'_>, operator: &Operator, mime_type: MimeType) -> std::io::Result<()> {
    delete_post_thumbnail(post, operator, ThumbnailCategory::Generated).await?;
    delete_post_thumbnail(post, operator, ThumbnailCategory::Custom).await?;
    delete_content(post, operator, mime_type).await
}

/// Renames the contents and thumbnails of two posts as if they had swapped ids.
pub async fn swap_posts(
    operator: &Operator,
    post_a: &PostHash<'_>,
    mime_type_a: MimeType,
    post_b: &PostHash<'_>,
    mime_type_b: MimeType,
) -> std::io::Result<()> {
    // No special cases needed here because generated thumbnails always exists and their type is always .jpg
    swap_files(operator, &post_a.generated_thumbnail_key(), &post_b.generated_thumbnail_key()).await?;

    // Handle the four distinct cases of custom thumbnails existing/not existing
    let custom_thumbnail_key_a = post_a.custom_thumbnail_key();
    let custom_thumbnail_key_b = post_b.custom_thumbnail_key();
    match (operator.exists(&custom_thumbnail_key_a).await.unwrap_or(false), operator.exists(&custom_thumbnail_key_b).await.unwrap_or(false)) {
        (true, true) => swap_files(operator, &custom_thumbnail_key_a, &custom_thumbnail_key_b).await?,
        (true, false) => move_file(operator, &custom_thumbnail_key_a, &custom_thumbnail_key_b).await?,
        (false, true) => move_file(operator, &custom_thumbnail_key_b, &custom_thumbnail_key_a).await?,
        (false, false) => (),
    }

    // Contents can have same MIME type or different MIME types
    let old_image_key_a = post_a.content_key(mime_type_a);
    let old_image_key_b = post_b.content_key(mime_type_b);
    if mime_type_a == mime_type_b {
        swap_files(operator, &old_image_key_a, &old_image_key_b).await
    } else {
        move_file(operator, &old_image_key_a, &post_b.content_key(mime_type_a)).await?;
        move_file(operator, &old_image_key_b, &post_a.content_key(mime_type_b)).await
    }
}

/// Moves file from `from` to `to`.
pub async fn move_file(operator: &Operator, from: &str, to: &str) -> std::io::Result<()> {
    operator.rename(from, to).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

/// Spawns an asynchronous task that periodically checks the temporary
/// upload directory for stale file uploads and deletes them.
pub fn spawn_temporary_uploads_cleanup_task(config: Arc<Config>) {
    const SWEEP_INTERVAL: Duration = Duration::from_hours(1);

    tokio::spawn(async move {
        let mut uploads = HashMap::new();
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            remove_stale_uploads(&config, &mut uploads);
        }
    });
}

/// Removes `file` if it exists.
fn remove_if_exists(file: &Path) -> std::io::Result<()> {
    if let Err(err) = std::fs::remove_file(file) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(err);
        }
    }
    Ok(())
}

async fn remove_if_exists_async(operator: &Operator, key: &str) -> std::io::Result<()> {
    match operator.delete(key).await {
        Ok(_) => Ok(()),
        Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }
}

/// Removes any stale files in the temporary uploads directory that are contained within `uploads`.
fn remove_stale_uploads(config: &Config, uploads: &mut HashMap<PathBuf, u64>) {
    let temporary_uploads_path = config.path(Directory::TemporaryUploads);
    let directory_iter = match std::fs::read_dir(temporary_uploads_path) {
        Ok(iter) => iter,
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                // Directory must have been deleted after startup. Clear uploads map
                uploads.clear();
            } else {
                warn!("Failed to cleanup temporary uploads directory: {err}");
            }
            return;
        }
    };

    let mut seen_files = HashSet::new();
    for file in directory_iter {
        let path = match file {
            Ok(entry) => entry.path(),
            Err(err) => {
                warn!("Failed to read directory entry: {err}");
                continue;
            }
        };
        let filesize = match path.metadata() {
            Ok(metadata) => metadata.len(),
            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    warn!("Failed to read metadata for {}: {err}", path.display());
                    seen_files.insert(path);
                }
                continue;
            }
        };

        match uploads.entry(path.clone()) {
            Entry::Occupied(mut entry) => {
                // If filesize has grown, assume file is still downloading and don't delete
                if filesize > *entry.get() {
                    *entry.get_mut() = filesize;
                    seen_files.insert(path);
                } else if let Err(err) = remove_if_exists(&path) {
                    warn!("Failed to remove {}: {err}", path.display());
                    seen_files.insert(path);
                } else {
                    entry.remove();
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(filesize);
                seen_files.insert(path);
            }
        }
    }

    // Drop entries for files that no longer exist
    uploads.retain(|path, _| seen_files.contains(path));
}

/// Swaps the names of two files.
async fn swap_files(operator: &Operator, file_a: &str, file_b: &str) -> std::io::Result<()> {
    let temp_path = format!("{}.tmp", file_a);
    move_file(operator, file_a, &temp_path).await?;
    move_file(operator, file_b, file_a).await?;
    move_file(operator, &temp_path, file_b).await
}

#[cfg(unix)]
/// Makes `path` readable to world. Used to avoid permissions issues on some systems.
fn set_permissions(path: &Path) -> std::io::Result<()> {
    const STANDARD_PERMISSIONS: u32 = 0o644;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(STANDARD_PERMISSIONS);
    std::fs::set_permissions(path, permissions)
}
