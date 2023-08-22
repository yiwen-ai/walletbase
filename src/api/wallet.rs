use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::db;

use crate::api::AppState;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct WalletOutput {
    pub uid: PackObject<xid::Id>,
    pub sequence: i64,
    pub award: i64,
    pub topup: i64,
    pub income: i64,
    pub credits: i64,
}

impl WalletOutput {
    pub fn from<T>(val: db::Wallet, to: &PackObject<T>) -> Self {
        Self {
            uid: to.with(val.uid),
            sequence: val.sequence,
            award: val.award,
            topup: val.topup,
            income: val.income,
            credits: val.credits,
        }
    }
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryWallet {
    pub uid: PackObject<xid::Id>,
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    Query(input): Query<QueryWallet>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "get_wallet".into()),
        ("uid", input.uid.to_string().into()),
    ])
    .await;

    let mut doc = db::Wallet::with_pk(input.uid.unwrap());
    doc.get_one(&app.scylla).await?;

    Ok(to.with(SuccessResponse::new(WalletOutput::from(doc, &to))))
}

// #[derive(Debug, Deserialize, Validate)]
// pub struct CreateTaskInput {
//     pub uid: PackObject<xid::Id>,
//     pub gid: PackObject<xid::Id>,
//     pub kind: String,
//     #[validate(range(min = 0, max = 256))]
//     pub threshold: i16,
//     #[validate(length(min = 0, max = 4))]
//     pub approvers: Vec<PackObject<xid::Id>>,
//     #[validate(length(min = 0, max = 256))]
//     pub assignees: Vec<PackObject<xid::Id>>,
//     pub message: String,
//     pub payload: PackObject<Vec<u8>>,
//     #[validate(range(min = -1, max = 2))]
//     pub group_role: Option<i8>,
// }

// pub async fn create(
//     State(app): State<Arc<AppState>>,
//     Extension(ctx): Extension<Arc<ReqContext>>,
//     to: PackObject<CreateTaskInput>,
// ) -> Result<PackObject<SuccessResponse<TaskOutput>>, HTTPError> {
//     let (to, input) = to.unpack();
//     input.validate()?;

//     ctx.set_kvs(vec![
//         ("action", "create_task".into()),
//         ("uid", input.uid.to_string().into()),
//         ("gid", input.gid.to_string().into()),
//         ("kind", input.kind.clone().into()),
//     ])
//     .await;

//     let mut doc = db::Task::with_pk(input.uid.unwrap(), xid::new());
//     doc.gid = input.gid.unwrap();
//     doc.status = 0i8;
//     doc.kind = input.kind;
//     doc.created_at = unix_ms() as i64;
//     doc.updated_at = doc.created_at;
//     doc.threshold = input.threshold;
//     doc.approvers = input.approvers.into_iter().map(|id| id.unwrap()).collect();
//     doc.assignees = input.assignees.into_iter().map(|id| id.unwrap()).collect();
//     doc.resolved = HashSet::new();
//     doc.rejected = HashSet::new();
//     doc.message = input.message;
//     doc.payload = input.payload.unwrap();

//     doc.save(&app.scylla).await?;

//     if let Some(role) = input.group_role {
//         let mut notif = db::GroupNotification::with_pk(doc.gid, doc.id, doc.uid);
//         notif.role = role;
//         let _ = notif.save(&app.scylla).await;
//     }
//     if !doc.approvers.is_empty() {
//         for id in &doc.approvers {
//             let mut notif = db::Notification::with_pk(*id, doc.id, doc.uid);
//             let _ = notif.save(&app.scylla).await;
//         }
//     }
//     if !doc.assignees.is_empty() {
//         for id in &doc.assignees {
//             let mut notif = db::Notification::with_pk(*id, doc.id, doc.uid);
//             let _ = notif.save(&app.scylla).await;
//         }
//     }

//     Ok(to.with(SuccessResponse::new(TaskOutput::from(doc, &to))))
// }

// #[derive(Debug, Deserialize, Validate)]
// pub struct AckTaskInput {
//     pub uid: PackObject<xid::Id>,
//     pub tid: PackObject<xid::Id>,
//     pub sender: PackObject<xid::Id>,
//     #[validate(range(min = -1, max = 1))]
//     pub status: i8,
//     pub message: String,
// }

// pub async fn ack(
//     State(app): State<Arc<AppState>>,
//     Extension(ctx): Extension<Arc<ReqContext>>,
//     to: PackObject<AckTaskInput>,
// ) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
//     let (to, input) = to.unpack();
//     input.validate()?;

//     if input.status != -1 && input.status != 1 {
//         return Err(HTTPError::new(
//             400,
//             format!("invalid status, expected -1 or 1, got {}", input.status),
//         ));
//     }
//     ctx.set_kvs(vec![
//         ("action", "ack_task".into()),
//         ("uid", input.uid.to_string().into()),
//         ("tid", input.tid.to_string().into()),
//         ("sender", input.sender.to_string().into()),
//     ])
//     .await;

//     let mut doc = db::Notification::with_pk(
//         input.uid.unwrap(),
//         input.tid.unwrap(),
//         input.sender.unwrap(),
//     );
//     doc.get_one(&app.scylla).await?;
//     if doc.status == input.status {
//         return Ok(to.with(SuccessResponse::new(false)));
//     }

//     let mut task = db::Task::with_pk(doc.sender, doc.tid);
//     if input.status == 1 {
//         task.update_resolved(&app.scylla, doc.uid).await?;
//     } else {
//         task.update_rejected(&app.scylla, doc.uid).await?;
//     }
//     doc.status = input.status;
//     doc.message = input.message;
//     doc.update(&app.scylla).await?;

//     Ok(to.with(SuccessResponse::new(true)))
// }

// pub async fn list(
//     State(app): State<Arc<AppState>>,
//     Extension(ctx): Extension<Arc<ReqContext>>,
//     to: PackObject<Pagination>,
// ) -> Result<PackObject<SuccessResponse<Vec<TaskOutput>>>, HTTPError> {
//     let (to, input) = to.unpack();
//     input.validate()?;

//     let page_size = input.page_size.unwrap_or(10);
//     ctx.set_kvs(vec![
//         ("action", "list_task".into()),
//         ("uid", input.uid.to_string().into()),
//         ("page_size", page_size.into()),
//     ])
//     .await;

//     let fields = input.fields.unwrap_or_default();
//     let res = db::Task::list(
//         &app.scylla,
//         input.uid.unwrap(),
//         fields,
//         page_size,
//         token_to_xid(&input.page_token),
//         input.status,
//     )
//     .await?;
//     let next_page_token = if res.len() >= page_size as usize {
//         to.with_option(token_from_xid(res.last().unwrap().id))
//     } else {
//         None
//     };

//     Ok(to.with(SuccessResponse {
//         total_size: None,
//         next_page_token,
//         result: res
//             .iter()
//             .map(|r| TaskOutput::from(r.to_owned(), &to))
//             .collect(),
//     }))
// }
