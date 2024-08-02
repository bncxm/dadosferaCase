# dadosferaCase

The initial architecture planned for the project involved batch processing of data using a Cloud Function, followed by storage in Cloud Storage, which would act as a data lake. The first insertion would be into the 'raw' layer, and then the data treatment and processing would be carried out, passing through the 'stage' layer and ultimately the 'curated' layer. With the data formatted and cleaned, BigQuery would be used to create views and dashboards, or analysis could be performed directly by the 'curated' layer using the chosen analysis tool.

![image](https://github.com/user-attachments/assets/5ea67219-14d3-4dc1-a7b4-849b51ef03e8)


However, due to lack of access to GCP, the project had to be adapted. The solution was to simulate the storage environment using MinIO to closely resemble the original architecture. A Docker application with MinIO was set up, and Python scripts were used, which could also function in cloud functions, to extract, process, and load data in the  'raw', 'stage', and 'curated' buckets. A significant difference was the need to download the Parquet file and save it locally for dashboard creation. Finally, data integration with Power BI is performed for descriptive and trend analyses.

![image](https://github.com/user-attachments/assets/756b7654-730e-4afe-b68e-3448ff89a32e)


The video I uploaded provides detailed explanations about the choice of tools and technologies used.
