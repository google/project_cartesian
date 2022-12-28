#!/bin/bash

# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


echo "Setting variable values..."
gcp_project_id=$(cat config.json | jq -r '.gcp_project_id')
bigquery_dataset=$(cat config.json | jq -r '.bigquery_dataset')
gcp_project_region=$(cat config.json | jq -r '.gcp_project_region')
cloud_run_service=$(cat config.json | jq -r '.cloud_run_service')
service_account_name=$(cat config.json | jq -r '.service_account_name')
bucket_name=$(cat config.json | jq -r '.bucket_name')

echo "Enabling required API services..."
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable shoppingcontent.googleapis.com

gcloud config set project "$gcp_project_id"

echo "Checking if BQ dataset exists..."
gcloud alpha bq datasets describe $bigquery_dataset || (echo "Creating BQ dataset..." && gcloud alpha bq datasets create $bigquery_dataset)

echo "Creating Service Account"
gcloud iam service-accounts create $service_account_name --display-name "Project Cartesian Service Account"
gcloud projects add-iam-policy-binding $gcp_project_id \
         --member serviceAccount:$service_account_name@$gcp_project_id.iam.gserviceaccount.com \
         --role roles/bigquery.admin

if ! [ -z "$bucket_name" ]
then
    echo "Creating Cloud Storage Bucket..."
    gsutil mb -p $gcp_project_id -l $gcp_project_region -b on gs://$bucket_name
fi


echo "Deploying Cloud Run..."
gcloud run deploy $cloud_run_service --region=$gcp_project_region --source="."

echo "Testing correct deployment..."
cloud_run_url=$(gcloud run services describe projectcartesian --platform managed --region $gcp_project_region --format 'value(status.url)')
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$cloud_run_url/test"
