#!/bin/bash

PROJECT=aqueous-choir-160420

gcloud -q --project=$PROJECT tasks queues create p1s6t1-replicate-datastore

gcloud -q --project=$PROJECT app deploy --version 1 3>> upload_log.txt
