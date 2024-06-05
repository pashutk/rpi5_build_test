use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use bson::{doc, Document};
use mongodb::{
    error::{BulkWriteError, BulkWriteFailure},
    options::{ClientOptions, InsertManyOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    env,
};

#[derive(Serialize, Deserialize)]
struct RequestData {
    db_collection: String,
    token: String,
    data: Vec<HashMap<String, serde_json::Value>>,
    id_field: String,
}

fn recover_not_inserted_indexes(
    error: mongodb::error::Error,
) -> Result<Vec<usize>, mongodb::error::Error> {
    match *error.clone().kind {
        mongodb::error::ErrorKind::BulkWrite(BulkWriteFailure {
            write_errors: Some(errors),
            ..
        }) => {
            if errors
                .clone()
                .into_iter()
                .all(|BulkWriteError { code, .. }| code == 11000)
            {
                Ok(errors
                    .into_iter()
                    .map(|BulkWriteError { index, .. }| index)
                    .collect())
            } else {
                Err(error)
            }
        }
        _ => Err(error),
    }
}

async fn process_data(data: web::Json<RequestData>) -> impl Responder {
    let env_access_token = env::var("ACCESS_TOKEN").unwrap();
    if env_access_token != data.token {
        return HttpResponse::Unauthorized().json(json!({
            "type": "error",
            "message": "Provide access token"
        }));
    }

    if !data
        .db_collection
        .starts_with(&env::var("MONGO_COLLECTIONS_PREFIX").unwrap())
    {
        return HttpResponse::BadRequest().json(json!({
            "type": "error",
            "message": "Can not write to this db collection"
        }));
    }

    let _ids_to_insert: Vec<&str> = data
        .data
        .iter()
        .filter_map(|obj| {
            obj.get(data.id_field.as_str()).and_then(|val| match val {
                serde_json::Value::String(id) => Some(id.as_str()),
                _ => None,
            })
        })
        .collect();

    let env_mongo_uri = env::var("MONGO_URI").unwrap();
    let client_options = ClientOptions::parse(env_mongo_uri).await.unwrap();
    let client = Client::with_options(client_options).unwrap();

    let collection = client
        .database(&env::var("MONGO_DB_NAME").unwrap())
        .collection::<Document>(&data.db_collection);

    let now = chrono::Utc::now();

    let data_objects_with_ids: Vec<_> = data
        .data
        .clone()
        .into_iter()
        .filter_map(|obj| obj.get(&data.id_field).cloned().map(|key| (key, obj)))
        .collect();

    let docs: Vec<Document> = data_objects_with_ids
        .clone()
        .into_iter()
        .map(|(id, obj)| {
            json!({
                "_id": id,
                "data": obj,
                "createdAt": bson::DateTime::from_chrono(now),
            })
        })
        .map(|value| bson::to_document(&value).unwrap())
        .collect();

    let docs_len = docs.len();

    let result = collection
        .insert_many(docs, InsertManyOptions::builder().ordered(false).build())
        .await;

    let inserted_indexes_result = result
        .map(|result| result.inserted_ids.into_keys().collect::<Vec<usize>>())
        .or_else(|error| {
            recover_not_inserted_indexes(error).map(|not_inserted_indexes| {
                let attempted_indexes: HashSet<usize> = (0..docs_len).collect();
                let not_inserted_indexes: HashSet<usize> =
                    not_inserted_indexes.into_iter().collect();
                let inserted_indexes = &attempted_indexes - &not_inserted_indexes;
                inserted_indexes.into_iter().collect()
            })
        })
        .map(|inserted_indexes| {
            let inserted_indexes_set: HashSet<_> = inserted_indexes.into_iter().collect();

            data_objects_with_ids
                .clone()
                .into_iter()
                .enumerate()
                .filter_map(|(index, (_id, obj))| {
                    inserted_indexes_set.contains(&index).then_some(json!(obj))
                })
                .collect::<Vec<serde_json::Value>>()
        });

    match inserted_indexes_result {
        Ok(inserted_jsons) => HttpResponse::Ok().json(json!({
            "type": "success",
            "data": inserted_jsons
        })),
        Err(err) => {
            eprintln!("Mongo insertMany error, {}", err);
            HttpResponse::InternalServerError().json(json!({
                "type": "error",
                "message": "Failed to write update to db"
            }))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env::var("ACCESS_TOKEN").expect("Set ACCESS_TOKEN env var!");
    env::var("MONGO_URI").expect("Set MONGO_URI env var!");
    env::var("MONGO_DB_NAME").expect("Set MONGO_DB_NAME env var!");
    env::var("MONGO_COLLECTIONS_PREFIX").expect("Set MONGO_COLLECTIONS_PREFIX env var!");

    HttpServer::new(|| {
        App::new()
            .service(web::resource("/data").route(web::post().to(process_data)))
            .service(web::redirect(
                "/",
                "https://github.com/pashutk/json-updates",
            ))
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}
