![image](https://github.com/AtilaKzlts/Application-Analyis/blob/main/assets/Bar-Temp.svg)

<div align="center"> <h1>UK Application analysis</h1> </p> </div>

## Project Introduction 

This study aims to analyze and visualize job applications from the southern region of the United Kingdom. The analysis covers key aspects such as position distribution, gender distribution, role distribution, experience levels, and desired salary ranges. The ultimate goal is to provide insights into the optimal location for a new office.  

## Executive Summary

An analysis of 3,270 job applications from the South of England reveals that the highest number of applications came from the Greater London region, totaling 971. The applicant pool consists of 53% male (1,719 applicants) and 47% female (1,551 applicants).  

The most frequently applied positions include:  
- **Laborer** – 287 applications  
- **Technician** – 203 applications  
- **Operator** – 179 applications  

Among specific roles, the **Unskilled Laborer** position received the highest number of applications (224). Candidates' experience levels range from 0 to 45 years, with the largest group having **5-10 years** of experience.  

Regarding salary expectations, the most commonly requested salary ranges are:  

1. **£55,000 - £70,000**  
2. **£70,000 - £85,000**  
3. **£45,000 - £55,000**  

The findings indicate that London and its surrounding areas dominate the applications, with a significant proportion of candidates having **medium-level experience**.  

## Steps  

![image](https://github.com/AtilaKzlts/Application-Analyis/blob/main/assets/diag.png)  

#### [View Script](https://github.com/AtilaKzlts/Application-Analyis/blob/main/assets/script.py)  


1. **Data Extraction**  
   Data was retrieved from a PostgreSQL database to initiate the analysis. The necessary job application data was extracted to form the foundation of the study.

2. **Data Cleaning & Processing**  
   PySpark on Google Colab was used to clean and process the data. Key steps included:  
   - **Summarizing applicant self-descriptions** and extracting **three key keywords** from each applicant's summary to identify key themes.  
   - **Summarizing past job experiences** into three relevant keywords to streamline the job history data and highlight important experiences.  
   - **Categorizing previous industries** based on predefined groups to standardize industry information and improve consistency in the analysis.  
   - **Column Cleaning**: Unnecessary and empty columns were removed, and rows with invalid data were dropped. Additionally, text content was cleaned by removing special characters, extra spaces, and unnecessary symbols.  
   - **Handling Missing Data**: Null values were checked across all columns, and missing data was either appropriately filled or rows were deleted. After cleaning, missing values were filled with "Unknown."  


3. **Text Summarization & Trend Analysis**  
   Language models were utilized to summarize text fields such as job and personal summaries. These summaries helped in identifying recurring trends across the applicants’ backgrounds and qualifications.

4. **Visualization**  
   Cleaned data was imported into **Tableau** for advanced visualizations, creating dashboards and reports that provide insights into applicant demographics, job roles, experience levels, and salary expectations.


## **Analysis Output**  

#### [View Full Report (PDF)](https://github.com/AtilaKzlts/Application-Analyis/blob/main/assets/report_pdf.pdf)  

![Dashboard Preview](https://github.com/AtilaKzlts/Application-Analyis/blob/main/assets/report.png)  

### [**Return to Portfolio**](https://github.com/AtilaKzlts/Atilla-Portfolio)  

---
