

<div align="center"> <h1>Strategic Site Selection</h1> </p> </div>

![image](https://github.com/AtilaKzlts/Application-Analyis/blob/main/assets/diag.png)  

## Table of Contents

  - Project Introduction
  - Executive Summary
  - Steps
  - Analysis Output

## Project Introduction 

Selecting the optimal location for a new regional office in the South of England is far more than a geographic exercise; it is a strategic decision that demands a granular understanding of the local talent ecosystem. By deconstructing 3,270 job applications, this analysis synthesizes labor density, mid-level expertise availability, and compensation benchmarks. This mapping provides a data-driven roadmap to identify a location that balances high talent accessibility with long-term financial and operational sustainability.

## Executive Summary

| **Stage / Area** | **Insight (Key Finding)** | **Recommended Action / Business Impact** |
| :--- | :--- | :--- |
| **Regional Dominance** | **Greater London** is the core talent hub with **971 applications** (~30%). London and its surrounding areas significantly dominate the candidate pool. | **Location Strategy:** Prioritize sites within the Greater London periphery to maximize reach. High concentration suggests a "Headquarters" model is viable here. |
| **Talent Density (Roles)** | **Laborer (287)**, **Technician (203)**, and **Operator (179)** are top roles. **Unskilled Laborer (224)** is the single highest specific role. | **Site Functionality:** The pool is skewed towards operational and technical roles. The new office should be optimized for technical operations and field management. |
| **Experience Sweet Spot** | Applicants range from 0-45 years, but the largest group is **5-10 years experience** (Mid-level). | **Recruitment Focus:** The region is ideal for hiring experienced mid-career professionals. Training programs for junior staff are less critical than leadership development. |
| **Compensation Reality** | Primary salary expectations are high: **£55k-£70k** (Rank 1) and **£70k-£85k** (Rank 2). | **Financial Feasibility:** Competitive benchmarking is mandatory. Budget for the new office must account for high cost-of-living adjustments inherent to the South. |
| **Demographic Balance** | Strong gender parity for a technical/labor pool: **53% Male (1,719)** and **47% Female (1,551)**. | **Corporate Social Responsibility:** High potential for building a diverse workforce. Leverage this balance to meet ESG goals and improve employer branding. |
| **Final Conclusion** | Market is dominated by **mid-level candidates** with **high salary expectations** primarily located in **London**. | **Decision Support:** A location in the **Greater London area** is optimal for talent access, provided the business model can support premium labor costs. |

## Steps  



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
