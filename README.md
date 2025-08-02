# Customer Sales ETL Pipeline with AWS Glue

## Overview

Built a scalable ETL pipeline using AWS Glue to ingest, transform, and analyze customer and sales data stored in Amazon S3. Used AWS Glue Crawlers for metadata generation and visual ETL jobs to identify high-value customers, storing results in a separate S3 bucket for analytics.

## Architecture

<img width="1001" height="356" alt="image" src="https://github.com/user-attachments/assets/b8af3969-a871-4a51-8a87-a2de10f5c187" />

## Key Steps

1. **Data Storage and Setup:**
   - Created dedicated Amazon S3 buckets for customers and sales datasets.
  ![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/5b36553e-2d52-4a3a-9132-39caa39b9629)

   - Established a storage bucket for storing the result of the AWS Glue job.

2. **Metadata Management with AWS Glue:**
   - Utilized AWS Glue crawler to automatically extract metadata from each CSV file. 
   - Established Data Catalog tables for efficient metadata management.
  ![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/9a7803ef-de9d-4c68-9822-1e9b83cc36f0)

  ![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/01eea3bd-67e4-465f-8d8d-4ff86c06d466)


3. **AWS Glue ETL Job:**
   - Developed an AWS Glue job utilizing metadata from Data Catalog tables.
  ![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/e77b2756-55ba-4974-9ce8-a57d1687f268)

   - Executed a join operation on 'customerID' and applied a filter for orders exceeding $800.

![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/85d9cbbb-d386-4343-97e8-e830ba252209)

4. **Data Transformation and Storage:**
   - Renamed key fields in the sales table for enhanced clarity.
   - Stored the transformed dataset in the 'customer-sales' folder within the designated S3 bucket.

  ![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/a1f2e8cb-093e-4494-808c-220cd56581f4)


5. **Parquet File Creation and Athena Querying:**
   - Generated Parquet files for the resulting dataset.
   - Utilized Amazon Athena for direct querying of the combined dataset stored in Parquet format, facilitating efficient analysis.
  
   ![image](https://github.com/Sameer1295/Customer-Sales-Data-Transform-GlueJob/assets/29782669/47b1ae62-b92d-45e5-8241-d8baf2488cdd)

