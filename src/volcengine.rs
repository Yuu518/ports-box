use std::time::Duration;

use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::provider::{Package, amount};

const HOST: &str = "billing.volcengineapi.com";
const REGION: &str = "cn-beijing";
const SERVICE: &str = "billing";
const QUERY: &str = "Action=ListResourcePackages&Version=2022-01-01";
const PRODUCT: &str = "TrafficPackage";
const PAGE_SIZE: &str = "20";
const MAX_PAGES: usize = 50;
const TIMEOUT: Duration = Duration::from_secs(15);

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn authorization(
    access_key: &str,
    secret_key: &str,
    x_date: &str,
    query: &str,
    body: &[u8],
) -> String {
    let canonical = format!(
        "POST\n/\n{query}\nhost:{HOST}\nx-date:{x_date}\n\nhost;x-date\n{}",
        sha256_hex(body)
    );
    let date = &x_date[..8];
    let scope = format!("{date}/{REGION}/{SERVICE}/request");
    let to_sign = format!(
        "HMAC-SHA256\n{x_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let key = hmac(secret_key.as_bytes(), date);
    let key = hmac(&key, REGION);
    let key = hmac(&key, SERVICE);
    let key = hmac(&key, "request");
    let signature = hex(&hmac(&key, &to_sign));
    format!(
        "HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders=host;x-date, Signature={signature}"
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Envelope {
    #[serde(default)]
    response_metadata: Metadata,
    #[serde(default)]
    result: Option<Page>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Metadata {
    #[serde(default)]
    error: Option<ApiError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ApiError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Page {
    #[serde(default)]
    list: Vec<Item>,
    #[serde(default)]
    next_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Item {
    instance_no: String,
    #[serde(default)]
    configuration_name: String,
    #[serde(default)]
    region_code: String,
    #[serde(default)]
    total_amount: String,
    #[serde(default)]
    available_amount: String,
    #[serde(default)]
    unit: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    expiry_time: String,
}

impl Item {
    fn into_package(self) -> Result<Package, String> {
        let parse = |value: &str| {
            amount(value, &self.unit).map_err(|e| format!("package {}: {e}", self.instance_no))
        };
        Ok(Package {
            total: parse(&self.total_amount)?,
            available: parse(&self.available_amount)?,
            id: self.instance_no,
            name: self.configuration_name,
            region: self.region_code,
            status: self.status,
            expiry: self.expiry_time,
        })
    }
}

fn describe(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut text = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

fn parse_page(status: u16, body: &str) -> Result<Page, String> {
    let envelope: Envelope = serde_json::from_str(body).map_err(|_| {
        let snippet: String = body.chars().take(200).collect();
        format!("unexpected response (http {status}): {snippet}")
    })?;
    if let Some(error) = envelope.response_metadata.error {
        return Err(format!("{}: {}", error.code, error.message));
    }
    envelope
        .result
        .ok_or_else(|| format!("response without a result (http {status})"))
}

pub async fn list_traffic_packages(
    access_key: &str,
    secret_key: &str,
) -> Result<Vec<Package>, String> {
    let client = reqwest::Client::builder()
        .timeout(TIMEOUT)
        .build()
        .map_err(|e| format!("cannot build http client: {e}"))?;
    let mut packages = Vec::new();
    let mut token = String::new();
    for _ in 0..MAX_PAGES {
        let body = json!({
            "ResourceType": "Package",
            "MaxResults": PAGE_SIZE,
            "NextToken": token,
            "Product": PRODUCT,
        })
        .to_string();
        let x_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let response = client
            .post(format!("https://{HOST}/?{QUERY}"))
            .header("X-Date", &x_date)
            .header("Content-Type", "application/json")
            .header(
                "Authorization",
                authorization(access_key, secret_key, &x_date, QUERY, body.as_bytes()),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", describe(e)))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|e| format!("cannot read response: {}", describe(e)))?;
        let page = parse_page(status, &text)?;
        let done = page.list.is_empty() || page.next_token.is_empty() || page.next_token == token;
        for item in page.list {
            packages.push(item.into_package()?);
        }
        if done {
            return Ok(packages);
        }
        token = page.next_token;
    }
    Err(format!("more than {MAX_PAGES} pages of traffic packages"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_published_example() {
        let header = authorization(
            "test-access-key",
            "WkRZeE1EQmxPVGhsWWpWak5HVmtNbUUxTXpZeU9UVXlOMlE1TmpZeVlqTQ==",
            "20250329T180937Z",
            "Action=ListBill&Version=2022-01-01",
            br#"{"Limit":10,"BillPeriod":"2023-08"}"#,
        );
        assert_eq!(
            header,
            "HMAC-SHA256 Credential=test-access-key/20250329/cn-beijing/billing/request, SignedHeaders=host;x-date, Signature=5e8480ceea12d0000a23c054151c50dd02c1a7dec835004057d19f13d53a7658"
        );
    }

    #[test]
    fn page_parses_packages() {
        let body = r#"{
            "ResponseMetadata": {"RequestId": "r", "Action": "ListResourcePackages"},
            "Result": {
                "List": [{
                    "InstanceNo": "tfp-a",
                    "Product": "TrafficPackage",
                    "ConfigurationName": "1TB",
                    "TotalAmount": "1024",
                    "AvailableAmount": "705.5",
                    "Unit": "GB",
                    "Status": "Effective",
                    "RegionCode": "cn-shanghai",
                    "ExpiryTime": "2027-06-28T15:59:59Z"
                }],
                "NextToken": ""
            }
        }"#;
        let page = parse_page(200, body).unwrap();
        assert!(page.next_token.is_empty());
        let package = page
            .list
            .into_iter()
            .next()
            .unwrap()
            .into_package()
            .unwrap();
        assert_eq!(
            package,
            Package {
                id: "tfp-a".into(),
                name: "1TB".into(),
                region: "cn-shanghai".into(),
                total: 1024 << 30,
                available: (705.5 * (1u64 << 30) as f64) as u64,
                status: "Effective".into(),
                expiry: "2027-06-28T15:59:59Z".into(),
            }
        );
    }

    #[test]
    fn api_errors_surface_code_and_message() {
        let body = r#"{"ResponseMetadata": {"Error": {"Code": "SignatureDoesNotMatch", "Message": "bad sig"}}}"#;
        let err = parse_page(401, body).err().unwrap();
        assert_eq!(err, "SignatureDoesNotMatch: bad sig");

        let err = parse_page(502, "<html>bad gateway</html>").err().unwrap();
        assert!(err.contains("http 502"), "{err}");
    }
}
