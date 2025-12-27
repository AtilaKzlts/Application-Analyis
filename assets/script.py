"""
UK Professional Data Cleaning Pipeline
========================================
This pipeline extracts data from PostgreSQL, performs comprehensive data cleaning,
validation, and transformation using PySpark.

Author: Atilla Kiziltas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, lower, regexp_replace, split, trim, 
    explode, udf, initcap, length, concat_ws, lit
)
from pyspark.sql.types import ArrayType, StringType
import nltk
from nltk.corpus import stopwords
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UKDataCleaningPipeline:
    """
    A comprehensive data cleaning pipeline for UK professional data.
    Handles data extraction from PostgreSQL, cleaning, validation, and export.
    """
    
    def __init__(self, db_config):
        """
        Initialize the pipeline with database configuration.
        
        Args:
            db_config (dict): Database connection parameters
        """
        self.db_config = db_config
        self.spark = None
        self.df = None
        self.stop_words = None
        self.custom_stopwords = None
        
        # Column definitions
        self.columns_to_drop_initial = [
            'Interests', 'Street Address', 'Phone numbers', 'Mobile', 'Metro', 
            'Middle Name', 'Birth Year', 'Middle Initial', 'Birth Date', 
            'Facebook Url', 'Facebook Username', 'Twitter Url', 'Twitter Username', 
            'Github Url', 'Github Username', 'Company Linkedin Url', 
            'Company Facebook Url', 'Company Twitter Url', 'Company Location Metro',
            'Company Location Address Line 2', 'Company Location Postal Code', 
            'Address Line 2', 'Postal Code', 'Industry 2', 'Company Industry'
        ]
        
        self.columns_to_drop_final = [
            'Industry', 'Location', 'First Name', 'Last Name', 
            'Company Location Name', 'Last Updated46', 'Last Updated55'
        ]
        
        self.columns_to_clean = [
            'Full name', 'Job title', 'Sub Role', 'Company Name', 
            'Locality', 'Region', 'Skills', 'Gender'
        ]
    
    def initialize_spark(self):
        """Initialize SparkSession with PostgreSQL JDBC driver."""
        logger.info("Initializing Spark Session...")
        
        self.spark = SparkSession.builder \
            .appName("UK Professional Data Cleaning Pipeline") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"Spark Session initialized successfully - Version: {self.spark.version}")
    
    def load_data_from_postgresql(self):
        """Load data from PostgreSQL database."""
        logger.info("Connecting to PostgreSQL database...")
        
        jdbc_url = f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        
        connection_properties = {
            "user": self.db_config['user'],
            "password": self.db_config['password'],
            "driver": "org.postgresql.Driver",
            "fetchsize": "10000"
        }
        
        try:
            self.df = self.spark.read \
                .jdbc(
                    url=jdbc_url,
                    table=self.db_config['table'],
                    properties=connection_properties
                )
            
            logger.info(f"Data loaded successfully - Rows: {self.df.count()}, Columns: {len(self.df.columns)}")
            
        except Exception as e:
            logger.error(f"Failed to load data from PostgreSQL: {str(e)}")
            raise
    
    def analyze_data_quality(self):
        """Analyze and log data quality metrics."""
        logger.info("Analyzing data quality...")
        
        # Count null values per column
        null_counts = self.df.select([
            count(when(col(c).isNull(), c)).alias(c) for c in self.df.columns
        ])
        
        logger.info("Null value counts per column:")
        null_counts.show(truncate=False)
        
        return null_counts
    
    def initial_cleaning(self):
        """Perform initial data cleaning operations."""
        logger.info("Starting initial data cleaning...")
        
        # Drop rows where all columns are NULL
        initial_count = self.df.count()
        self.df = self.df.dropna(how="all")
        logger.info(f"Removed {initial_count - self.df.count()} completely empty rows")
        
        # Drop unnecessary columns
        self.df = self.df.drop(*self.columns_to_drop_initial)
        logger.info(f"Dropped {len(self.columns_to_drop_initial)} unnecessary columns")
    
    def initialize_nlp_components(self):
        """Initialize NLP components for text processing."""
        logger.info("Initializing NLP components...")
        
        try:
            nltk.download("stopwords", quiet=True)
            self.stop_words = set(stopwords.words("english"))
            self.custom_stopwords = self.stop_words.union({
                "and", "with", "for", "in", "at", "of", "on", "to", "by"
            })
            logger.info("NLP components initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize NLP components: {str(e)}")
            raise
    
    def process_industry_classification(self):
        """Process and classify industry data using text analysis."""
        logger.info("Processing industry classification...")
        
        # Define UDF for stopword removal
        def remove_stopwords(word_list):
            return [word for word in word_list if word not in self.custom_stopwords] if word_list else []
        
        remove_stopwords_udf = udf(remove_stopwords, ArrayType(StringType()))
        
        # Text cleaning and tokenization
        df_cleaned = self.df.withColumn(
            "cleaned_text", 
            lower(regexp_replace(col("Industry"), "[^a-zA-Z\\s]", ""))
        )
        
        df_tokenized = df_cleaned.withColumn(
            "words", 
            split(trim(col("cleaned_text")), "\\s+")
        )
        
        df_filtered = df_tokenized.withColumn(
            "filtered_words", 
            remove_stopwords_udf(col("words"))
        )
        
        # Word frequency analysis
        df_exploded = df_filtered.select(explode(col("filtered_words")).alias("word"))
        df_word_counts = df_exploded.groupBy("word").agg(count("*").alias("count"))
        df_frequent_words = df_word_counts.filter(col("count") >= 5)
        
        frequent_words_list = df_frequent_words.select("word").rdd.flatMap(lambda x: x).collect()
        logger.info(f"Identified {len(frequent_words_list)} frequent industry keywords")
        
        # Industry matching
        def match_industry(text):
            if not text:
                return None
            text_lower = text.lower()
            return next((word for word in frequent_words_list if word in text_lower), None)
        
        match_industry_udf = udf(match_industry, StringType())
        self.df = self.df.withColumn("matched_industry", match_industry_udf(col("Industry")))
    
    def validate_and_clean_fields(self):
        """Validate and clean specific data fields."""
        logger.info("Validating and cleaning data fields...")
        
        # Fill null values
        self.df = self.df.fillna('Unknown')
        
        # Clean and capitalize text columns
        for column_name in self.columns_to_clean:
            self.df = self.df.withColumn(
                column_name, 
                initcap(regexp_replace(col(column_name), '[^a-zA-Z\\s]', ''))
            )
        
        # Validate Full name length
        self.df = self.df.withColumn(
            'Full name',
            when(length(col('Full name')) > 34, 'Unknown').otherwise(col('Full name'))
        )
        
        # Email validation
        def validate_email(email):
            if not email:
                return "unknown"
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return email if re.match(pattern, email) else "unknown"
        
        validate_email_udf = udf(validate_email, StringType())
        self.df = self.df.withColumn("Email", validate_email_udf(col("Emails")))
        
        # Clean Company Size
        self.df = self.df.withColumn(
            'Company Size',
            when(
                col('Company Size').rlike('^[0-9]+$') | 
                col('Company Size').rlike('^[0-9]+[-+][0-9]*$'), 
                col('Company Size')
            ).otherwise('Unknown')
        )
        
        # Clean Company Founded
        self.df = self.df.withColumn(
            'Company Founded',
            when(col('Company Founded').rlike('^[0-9]{4}$'), col('Company Founded'))
            .otherwise('Unknown')
        )
        
        # Validate Job title length
        self.df = self.df.withColumn(
            'Job title',
            when(length(col('Job title')) > 40, 'Unknown').otherwise(col('Job title'))
        )
        
        logger.info("Field validation and cleaning completed")
    
    def extract_keywords_from_summaries(self):
        """Extract key terms from job and personal summaries."""
        logger.info("Extracting keywords from summaries...")
        
        def extract_keywords(text):
            if not text:
                return []
            
            # Clean text
            text = text.lower()
            for char in ['.', ',', ':', ';']:
                text = text.replace(char, '')
            
            words = text.split()
            filtered_words = [word for word in words if word not in self.custom_stopwords]
            
            # Count word frequencies
            word_counts = {word: filtered_words.count(word) for word in set(filtered_words)}
            
            # Return top 3 keywords
            return sorted(word_counts, key=word_counts.get, reverse=True)[:3]
        
        extract_keywords_udf = udf(extract_keywords, ArrayType(StringType()))
        
        self.df = self.df.withColumn(
            "job_sum_key_word", 
            extract_keywords_udf(col("Job Summary"))
        )
        
        self.df = self.df.withColumn(
            "personal_sum_keyword", 
            extract_keywords_udf(col("Summary"))
        )
        
        # Convert arrays to comma-separated strings
        self.df = self.df.withColumn(
            "job_sum_key_word", 
            concat_ws(",", col("job_sum_key_word"))
        )
        
        self.df = self.df.withColumn(
            "personal_sum_keyword", 
            concat_ws(",", col("personal_sum_keyword"))
        )
        
        logger.info("Keyword extraction completed")
    
    def finalize_dataset(self):
        """Remove unnecessary columns and finalize the dataset."""
        logger.info("Finalizing dataset...")
        
        self.df = self.df.drop(*self.columns_to_drop_final)
        logger.info(f"Final dataset prepared - Columns: {len(self.df.columns)}")
    
    def export_results(self, output_path):
        """Export cleaned data to Excel format."""
        logger.info(f"Exporting results to {output_path}...")
        
        try:
            df_pandas = self.df.toPandas()
            df_pandas.to_excel(output_path, index=False, engine='openpyxl')
            logger.info(f"Data exported successfully to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export data: {str(e)}")
            raise
    
    def display_sample(self, num_rows=10):
        """Display sample of cleaned data."""
        logger.info(f"Displaying {num_rows} sample rows:")
        self.df.show(num_rows, truncate=False)
    
    def run_pipeline(self, output_path):
        """
        Execute the complete data cleaning pipeline.
        
        Args:
            output_path (str): Path for output Excel file
        """
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("Starting UK Data Cleaning Pipeline")
        logger.info("=" * 80)
        
        try:
            # Pipeline steps
            self.initialize_spark()
            self.load_data_from_postgresql()
            self.analyze_data_quality()
            self.initial_cleaning()
            self.initialize_nlp_components()
            self.process_industry_classification()
            self.validate_and_clean_fields()
            self.extract_keywords_from_summaries()
            self.finalize_dataset()
            self.display_sample()
            self.export_results(output_path)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session terminated")


def main():
    """Main execution function."""
    
    # Database configuration
    db_config = {
        'host': 'XXX',
        'port': 'XXX',
        'database': 'XXX',
        'user': 'XXX',
        'password': 'XXX',
        'table': 'XXX'
    }
    
    # Output configuration
    output_path = '/opt/data/outputs/uk_cleaned_data.xlsx'
    
    # Initialize and run pipeline
    pipeline = UKDataCleaningPipeline(db_config)
    pipeline.run_pipeline(output_path)


if __name__ == "__main__":
    main()