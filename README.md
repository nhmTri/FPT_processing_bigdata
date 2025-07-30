# FPT_processing_bigdata
Engineered a high-performance PySpark pipeline to process over 10 million raw JSON records per batch, generating 1+ million OLAP-ready rows in under 250 seconds.

# Sample Video
https://drive.google.com/file/d/1CPp5jQS-OPERCuMbpCZSnlkHCB0UMFH3/view?usp=sharing

# Olap Output
<img width="1030" height="522" alt="Image" src="https://github.com/user-attachments/assets/f872ec2a-77d4-4f71-b3a5-a07dd1ecf447" />

# Column Descriptions
| Column         | Description |
|----------------|-------------|
| **Contract**     | Unique identifier of the user (e.g., IP address or contract ID). |
| **Activeness**   | User's activity level: `Low Active` or `High Active`. |
| **Giai Tri**     | Total time spent watching **entertainment** channels. |
| **Phim Truyen**  | Total time spent watching **movie/drama** channels. |
| **The Thao**     | Total time spent watching **sports** channels. |
| **Thieu Nhi**    | Total time spent watching **children's** channels. |
| **Truyen Hinh**  | Total time spent watching **general TV** channels. |
| **MostWatch**    | The **most-watched content type** by the user during the recorded period. |
| **Taste**        | A list of **content categories** that the user interacted with. |

