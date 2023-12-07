extern crate aw_models;
extern crate chrono;
extern crate gethostname;
extern crate reqwest;
extern crate serde_json;
extern crate tokio;

pub mod blocking;

use std::vec::Vec;
use std::{collections::HashMap, error};

use chrono::{DateTime, Utc};
use serde_json::Map;

pub use aw_models::{Bucket, BucketMetadata, Event};
pub use reqwest::Error as RequestError;
pub use reqwest::Url;

pub struct AwClient {
    client: reqwest::Client,
    pub baseurl: Url,
    pub name: String,
    pub hostname: String,
}

impl std::fmt::Debug for AwClient {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AwClient(baseurl={:?})", self.baseurl)
    }
}

fn get_hostname() -> String {
    return gethostname::gethostname().to_string_lossy().to_string();
}

impl AwClient {
    pub fn new(host: &str, port: u16, name: &str) -> Result<AwClient, Box<dyn error::Error>> {
        let baseurl = Url::parse(&format!("http://{}:{}", host, port))?;
        let hostname = get_hostname();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()?;

        Ok(AwClient {
            client,
            baseurl,
            name: name.to_string(),
            hostname,
        })
    }

    pub async fn get_bucket(&self, bucketname: &str) -> Result<Bucket, RequestError> {
        let url = format!("{}/api/0/buckets/{}", self.baseurl, bucketname);
        let bucket = self
            .client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(bucket)
    }

    pub async fn get_buckets(&self) -> Result<HashMap<String, Bucket>, RequestError> {
        let url = format!("{}/api/0/buckets/", self.baseurl);
        self.client.get(url).send().await?.json().await
    }

    pub async fn create_bucket(&self, bucket: &Bucket) -> Result<(), RequestError> {
        let url = format!("{}/api/0/buckets/{}", self.baseurl, bucket.id);
        self.client.post(url).json(bucket).send().await?;
        Ok(())
    }

    pub async fn create_bucket_simple(
        &self,
        bucketname: &str,
        buckettype: &str,
    ) -> Result<(), RequestError> {
        let bucket = Bucket {
            bid: None,
            id: bucketname.to_string(),
            client: self.name.clone(),
            _type: buckettype.to_string(),
            hostname: self.hostname.clone(),
            data: Map::default(),
            metadata: BucketMetadata::default(),
            events: None,
            created: None,
            last_updated: None,
        };
        self.create_bucket(&bucket).await
    }

    pub async fn delete_bucket(&self, bucketname: &str) -> Result<(), RequestError> {
        let url = format!("{}/api/0/buckets/{}", self.baseurl, bucketname);
        self.client.delete(url).send().await?;
        Ok(())
    }

    pub async fn get_events(
        &self,
        bucketname: &str,
        start: Option<DateTime<Utc>>,
        stop: Option<DateTime<Utc>>,
        limit: Option<u64>,
    ) -> Result<Vec<Event>, RequestError> {
        let mut url = Url::parse(
            format!("{}/api/0/buckets/{}/events", self.baseurl, bucketname).as_str(),
        )
        .unwrap();

        // Must be a better way to build URLs
        if let Some(s) = start {
            url.query_pairs_mut()
                .append_pair("start", s.to_rfc3339().as_str());
        };
        if let Some(s) = stop {
            url.query_pairs_mut()
                .append_pair("end", s.to_rfc3339().as_str());
        };
        if let Some(s) = limit {
            url.query_pairs_mut()
                .append_pair("limit", s.to_string().as_str());
        };
        self.client.get(url).send().await?.json().await
    }

    pub async fn insert_event(&self, bucketname: &str, event: &Event) -> Result<(), RequestError> {
        let url = format!("{}/api/0/buckets/{}/events", self.baseurl, bucketname);
        let eventlist = vec![event.clone()];
        self.client.post(url).json(&eventlist).send().await?;
        Ok(())
    }

    pub async fn insert_events(&self, bucketname: &str, events: Vec<Event>) -> Result<(), RequestError> {
        let url = format!("{}/api/0/buckets/{}/events", self.baseurl, bucketname);
        self.client.post(url).json(&events).send().await?;
        Ok(())
    }

    pub async fn heartbeat(
        &self,
        bucketname: &str,
        event: &Event,
        pulsetime: f64,
    ) -> Result<(), RequestError> {
        let url = format!(
            "{}/api/0/buckets/{}/heartbeat?pulsetime={}",
            self.baseurl, bucketname, pulsetime
        );
        self.client.post(url).json(&event).send().await?;
        Ok(())
    }

    pub async fn delete_event(&self, bucketname: &str, event_id: i64) -> Result<(), RequestError> {
        let url = format!(
            "{}/api/0/buckets/{}/events/{}",
            self.baseurl, bucketname, event_id
        );
        self.client.delete(url).send().await?;
        Ok(())
    }

    pub async fn get_event_count(&self, bucketname: &str) -> Result<i64, RequestError> {
        let url = format!("{}/api/0/buckets/{}/events/count", self.baseurl, bucketname);
        let res = self
            .client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let count: i64 = match res.trim().parse() {
            Ok(count) => count,
            Err(err) => panic!("could not parse get_event_count response: {err:?}"),
        };
        Ok(count)
    }

    pub async fn get_info(&self) -> Result<aw_models::Info, RequestError> {
        let url = format!("{}/api/0/info", self.baseurl);
        self.client.get(url).send().await?.json().await
    }
}
