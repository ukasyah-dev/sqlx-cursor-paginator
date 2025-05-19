use base64::{Engine, prelude::BASE64_URL_SAFE};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_util::error;
use sqlx::{FromRow, Pool, Postgres, QueryBuilder, postgres::PgRow};
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PaginationRequest {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub sort_by: Option<String>,
    #[param(inline)]
    pub sort_order: Option<SortOrder>,
}

#[derive(Default, Serialize, ToSchema)]
pub struct PaginationResponse<T> {
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
}

pub struct Paginator<T> {
    keys_: (String, String),
    retrieve_keys_: fn(&T) -> (String, String),
    request_: PaginationRequest,
}

impl<'a, T> Paginator<T>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Sync + Unpin,
{
    pub fn new() -> Self {
        Paginator {
            keys_: (String::from(""), String::from("")),
            retrieve_keys_: |_: &T| (String::from(""), String::from("")),
            request_: PaginationRequest::default(),
        }
    }

    pub fn keys(mut self, key1: &str, key2: &str) -> Self {
        self.keys_ = (key1.to_string(), key2.to_string());
        self
    }

    pub fn retrieve_keys(mut self, f: fn(&T) -> (String, String)) -> Self {
        self.retrieve_keys_ = f;
        self
    }

    pub fn request(mut self, request: &PaginationRequest) -> Self {
        self.request_ = request.clone();
        self
    }

    pub async fn paginate<K1, K2>(
        self,
        db: &Pool<Postgres>,
        mut query: QueryBuilder<'a, Postgres>,
    ) -> Result<PaginationResponse<T>, error::Error>
    where
        K1: 'a
            + KeyParse
            + std::default::Default
            + sqlx::Encode<'a, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>
            + Send,
        K2: 'a
            + KeyParse
            + std::default::Default
            + sqlx::Encode<'a, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>
            + Send,
    {
        let mut cursor_values: Vec<String> = vec![];
        if let Some(cursor) = self.request_.cursor {
            cursor_values = parse_cursor(cursor)?;
        }

        let mut smaller = true;
        if let Some(o) = self.request_.sort_order.clone() {
            smaller = o == SortOrder::Desc;
        }

        let mut limit = 10;
        if let Some(l) = self.request_.limit {
            if l > 0 && l <= 100 {
                limit = l;
            }
        }

        let (key1, key2) = self.keys_.clone();

        // we add 1 to limit to ensure there's a next page (the extra record will be discarded)
        let pagination = sqlx_page::Pagination::new(smaller, limit + 1, vec![key1, key2]);

        if cursor_values.len() == 2 {
            query.push(" AND");

            let key1: K1 = parse_key(cursor_values[0].clone())?;
            let key2: K2 = parse_key(cursor_values[1].clone())?;

            pagination.push_where2(&mut query, Some((key1, key2)));
        }

        pagination.push_order_by(&mut query);
        pagination.push_limit(&mut query);

        let data = match query.build_query_as::<T>().fetch_all(db).await {
            Ok(data) => data,
            Err(err) => {
                log::error!("failed to run pagination query: {}", err);
                return Err(error::internal());
            }
        };

        let mut res: PaginationResponse<T> = PaginationResponse {
            data,
            next_cursor: None,
        };

        // if we got limit+1 records, we have a next page
        if res.data.len() == (limit as usize) + 1 {
            res.data.remove(res.data.len() - 1);

            if let Some(last) = res.data.last() {
                let (key1, key2) = (self.retrieve_keys_)(last);
                let keys = vec![key1, key2];

                let cursor_json = match serde_json::to_vec(&keys) {
                    Ok(cursor_json) => cursor_json,
                    Err(err) => {
                        log::error!("failed to serialize next cursor: {}", err);
                        return Err(error::internal());
                    }
                };

                let cursor = BASE64_URL_SAFE.encode(&cursor_json).to_string();
                res.next_cursor = Some(cursor);
            }
        }

        Ok(res)
    }
}

fn parse_cursor(cursor: String) -> Result<Vec<String>, error::Error> {
    let bytes = match BASE64_URL_SAFE.decode(&cursor) {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err(error::invalid_argument_with_message("invalid cursor"));
        }
    };

    let values: Vec<String> = match serde_json::from_slice(&bytes) {
        Ok(values) => values,
        Err(_) => {
            return Err(error::invalid_argument_with_message("invalid cursor"));
        }
    };

    if values.len() != 2 {
        return Err(error::invalid_argument_with_message("invalid cursor"));
    }

    Ok(values)
}

pub trait KeyParse: Sized {
    fn parse_key(key: String) -> Result<Self, error::Error>;
}

impl KeyParse for String {
    fn parse_key(key: String) -> Result<String, error::Error> {
        Ok(key)
    }
}

impl KeyParse for DateTime<Utc> {
    fn parse_key(key: String) -> Result<DateTime<Utc>, error::Error> {
        let res: DateTime<Utc> = match key.parse::<DateTime<Utc>>() {
            Ok(res) => res,
            Err(_) => {
                return Err(error::invalid_argument_with_message(
                    "failed to parse date time from string",
                ));
            }
        };
        Ok(res)
    }
}

fn parse_key<K: KeyParse>(key: String) -> Result<K, error::Error> {
    K::parse_key(key)
}
