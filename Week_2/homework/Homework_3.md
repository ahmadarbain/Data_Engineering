## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770 [+]
* 766,792
* 299,234
* 822,132

**Answer:**
<br>
```
(zoom) PS D:\Data_Engineering> python .\Week_2\flows\02_gcp\etl_web_to_gcs.py
00:15:45.425 | INFO    | prefect.engine - Created flow run 'majestic-kiwi' for f
low 'etl-web-to-gcs'
00:15:45.734 | INFO    | Flow run 'majestic-kiwi' - Created task run 'fetch-0' f
or task 'fetch'
00:15:45.738 | INFO    | Flow run 'majestic-kiwi' - Executing 'fetch-0' immediat
ely...
D:\Data_Engineering\Week_2\flows\02_gcp\etl_web_to_gcs.py:13: DtypeWarning: Colu
mns (3) have mixed types. Specify dtype option on import or set low_memory=False
.
  df = pd.read_csv(dataset_url)
00:15:49.368 | INFO    | Task run 'fetch-0' - Finished in state Completed()
00:15:49.445 | INFO    | Flow run 'majestic-kiwi' - Created task run 'clean-0' f
or task 'clean'
00:15:49.447 | INFO    | Flow run 'majestic-kiwi' - Executing 'clean-0' immediat
ely...
   VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0       2.0  2019-12-18 15:52:30  ...       1.0                  0.0
1       2.0  2020-01-01 00:45:58  ...       2.0                  0.0

[2 rows x 20 columns]
columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
row: 447770
00:15:50.048 | INFO    | Task run 'clean-0' - Finished in state Completed()
00:15:50.127 | INFO    | Flow run 'majestic-kiwi' - Created task run 'write_loca
l-0' for task 'write_local'
00:15:50.129 | INFO    | Flow run 'majestic-kiwi' - Executing 'write_local-0' im
mediately...
00:15:51.337 | INFO    | Task run 'write_local-0' - Finished in state Completed(
)
00:15:51.410 | INFO    | Flow run 'majestic-kiwi' - Created task run 'write_gcs-
0' for task 'write_gcs'
00:15:51.413 | INFO    | Flow run 'majestic-kiwi' - Executing 'write_gcs-0' imme
diately...
00:15:51.595 | INFO    | Task run 'write_gcs-0' - Getting bucket 'prefect-ar-de-
zoomcamp'.
00:15:52.096 | INFO    | Task run 'write_gcs-0' - Uploading from 'data/green-gre
en_tripdata_2020-01.parquet' to the bucket 'prefect-ar-de-zoomcamp' path 'data/g
reen-green_tripdata_2020-01.parquet'.
00:16:06.438 | INFO    | Task run 'write_gcs-0' - Finished in state Completed()
00:16:06.508 | INFO    | Flow run 'majestic-kiwi' - Finished in state Completed(
'All states completed.')
```


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *` [+]
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

**Answer**
</br>
```
(zoom) PS D:\Data_Engineering> prefect deployment build .\Week_2\flows\02_gcp\et
l_web_to_gcs.py:etl_web_to_gcs -n etl2 --cron "0 5 1 * *"
Found flow 'etl-web-to-gcs'
Deployment YAML created at 
'D:\Data_Engineering\etl_web_to_gcs-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  
Pass --skip-upload to suppress this warning.
```

```
(zoom) PS D:\Data_Engineering> prefect deployment apply .\etl_web_to_gcs-deploym
ent.yaml
Successfully loaded 'etl2'
Deployment 'etl-web-to-gcs/etl2' successfully created with id 
'8aaea271-aa8e-46da-9ad4-2bdb906aa402'.
View Deployment in UI:
http://127.0.0.1:4200/deployments/deployment/8aaea271-aa8e-46da-9ad4-2bdb906aa4
02

To execute flow runs from this deployment, start an agent that pulls work from
the 'default' work queue:
$ prefect agent start -q 'default'
```

</br>

**Output**

![Output](./../images/pic-1.png)

</br>

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920 [+]
- 12,282,990
- 27,235,753
- 11,338,483


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225



## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10


## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 8 February (Wednesday), 22:00 CET


## Solution

We will publish the solution here