use std::sync::Arc;

use crate::{
    config::{asst::AsstConfig, task::TaskConfig},
    run::{
        callback::summary::{self, SummarySubscriber},
        external, find_profile, resource, CommonArgs,
    },
};
use anyhow::{Context, Result};
use maa_dirs::{self as dirs};

use maa_server::{
    core::core_client::CoreClient,
    prelude::*,
    task::{task_client::TaskClient, NewTaskRequest},
};
use maa_types::TaskStateType;
use tempfile::NamedTempFile;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

#[tokio::main]
pub async fn run_core<F>(f: F, args: CommonArgs, rx: &mut SummarySubscriber) -> Result<()>
where
    F: FnOnce(&AsstConfig) -> Result<TaskConfig>,
{
    // Auto update hot update resource
    resource::update(true)?;

    let socket = Arc::new(NamedTempFile::new()?.into_temp_path());
    tracing::info!("Socket: {}", socket.display());

    let cancel_token = CancellationToken::new();
    tokio::select! {
        () = super::host::start_daemon(Arc::clone(&socket), cancel_token.child_token()) => { },
        ret = run_client(f, socket,  args, rx) => { ret? },
        () = super::host::wait_for_signal() => {  },
    }
    cancel_token.cancel();

    Ok(())
}

async fn run_client<F>(
    f: F,
    socket: Arc<tempfile::TempPath>,
    args: CommonArgs,
    rx: &mut SummarySubscriber,
) -> Result<()>
where
    F: FnOnce(&AsstConfig) -> Result<TaskConfig>,
{
    // Load asst config
    let mut asst_config = find_profile(dirs::config(), args.profile.as_deref())?;

    args.apply_to(&mut asst_config);

    let task = f(&asst_config)?;
    let task_config = task.init()?;
    if let Some(resource) = task_config.client_type.resource() {
        asst_config.resource.use_global_resource(resource);
    }

    let channel = Endpoint::from_static("http://127.0.0.1:50051")
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let path = Arc::clone(&socket);
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    tokio::net::UnixStream::connect(&*path).await?,
                ))
            }
        }))
        .await?;

    let mut coreclient = CoreClient::new(channel.clone());
    load_and_setup_core(&asst_config, &mut coreclient).await?;

    let mut taskclient = TaskClient::new(channel);

    // Launch external app like PlayCover or Emulator
    // Only support PlayCover on macOS now, may support more in the future
    let app: Option<Box<dyn external::ExternalApp>> = match asst_config.connection.preset() {
        #[cfg(target_os = "macos")]
        crate::config::asst::Preset::PlayCover => Some(Box::new(external::PlayCoverApp::new(
            task_config.client_type,
            address.as_ref(),
        ))),
        _ => None,
    };

    // Startup external app
    if let (Some(app), true) = (app.as_deref(), task_config.start_app) {
        app.open().await.context("Failed to open external app")?;
    }
    
    let session_id = taskclient
        .new_connection(maa_server::task::NewConnectionRequest {
            conncfg: Some(asst_config.connection.into()),
            instcfg: Some(asst_config.instance_options.into()),
        })
        .await
        .map(|resp| resp.into_inner())?;

    for task in task_config.tasks {
        let task_type = task.task_type;
        let task_params = serde_json::to_string(&task.params)?;
        tracing::debug!(
            "Adding task [{}] with params: {task_params}",
            task.name_or_default(),
        );

        let id = taskclient
            .append_task(make_request(
                NewTaskRequest {
                    task_type: task_type.into(),
                    task_params,
                },
                &session_id,
            ))
            .await
            .with_context(|| {
                let task_params = serde_json::to_string_pretty(&task.params).unwrap();
                format!(
                    "Failed to add task {} with params: {task_params}",
                    task.name_or_default(),
                )
            })?
            .into_inner()
            .id;

        tracing::info!("New task[{}]({})", task_type, id);
        summary::insert(id, task.name, task_type);
    }

    if !args.dry_run {
        taskclient
            .start_tasks(make_request((), &session_id))
            .await?;

        let mut ch = taskclient
            .task_state_update(make_request((), &session_id))
            .await?
            .into_inner();

        while let Some(st) = ch.next().await {
            let st = st.unwrap();
            let code = st.state();
            let json = serde_json::from_str(&st.content).unwrap();
            super::super::callback::process_message(code.into(), json);
            if let Some(updated) = rx.try_update() {
                print!("{}", updated)
            }
            if code == TaskStateType::AllTasksCompleted {
                break;
            }
        }

        taskclient.stop_tasks(make_request((), &session_id)).await?;
    }

    taskclient
        .close_connection(make_request((), &session_id))
        .await?;

    // Close external app
    if let (Some(app), true) = (app.as_deref(), task_config.close_app) {
        app.close().await.context("Failed to close external app")?;
    }

    if !coreclient.unload_core(()).await?.into_inner() {
        tracing::warn!("Failed to shutdown server");
    }
    Ok(())
}

fn make_request<T>(payload: T, session_id: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(payload);
    req.metadata_mut()
        .insert(HEADER_SESSION_ID, session_id.parse().unwrap());
    req
}

async fn load_and_setup_core(
    config: &AsstConfig,
    coreclient: &mut CoreClient<Channel>,
) -> Result<()> {
    let success = coreclient
        .load_core(maa_server::core::CoreConfig {
            static_ops: Some(config.static_options.clone().into()),
            log_ops: Some(maa_server::core::core_config::LogOptions {
                path: "/home/maa/".to_owned(),
                level: maa_server::core::core_config::LogLevel::Debug.into(),
            }),
            lib_path: maa_dirs::find_library()
                .map(|path| {
                    path.join(maa_dirs::MAA_CORE_LIB)
                        .to_str()
                        .unwrap()
                        .to_owned()
                })
                .unwrap_or(maa_dirs::MAA_CORE_LIB.to_owned()),
            resource_dirs: config
                .resource
                .resource_dirs()
                .iter()
                .map(|dir| dir.to_str().unwrap().to_owned())
                .collect(),
        })
        .await?
        .into_inner();

    if !success {
        tracing::warn!("Core has been configured");
    }
    Ok(())
}
