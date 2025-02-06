from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType
import nltk
from nltk.corpus import stopwords
import re
from geopy.geocoders import Nominatim

# Start SparkSession
spark = SparkSession.builder \
    .appName("Colab PySpark Example") \
    .getOrCreate()

df = spark.read.csv('/content/UnitedKingdom_0.csv', header=True, inferSchema=True)

# Row and column count
print(f"Row count: {df.count()}")
print(f"Column count: {len(df.columns)}")

# Count of null values per column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Drop rows where all columns are NULL
df = df.dropna(how="all")

# Drop unnecessary columns
columns_to_drop = [
    'Interests', 'Street Address', 'Phone numbers', 'Mobile', 'Metro', 'Middle Name',
    'Birth Year', 'Middle Initial', 'Birth Date', 'Facebook Url',
    'Facebook Username', 'Twitter Url', 'Twitter Username', 'Github Url', 'Github Username',
    'Company Linkedin Url', 'Company Facebook Url', 'Company Twitter Url', 'Company Location Metro',
    'Company Location Address Line 2', 'Company Location Postal Code', 'Address Line 2',
    'Postal Code', 'Industry 2', 'Company Industry'
]
df = df.drop(*columns_to_drop)

# Load stopwords
nltk.download("stopwords")
stop_words = set(stopwords.words("english"))
custom_stopwords = stop_words.union({"and", "with", "for", "in", "at", "of", "on", "to", "by"})

# Clean text, tokenize, and remove stopwords
def remove_stopwords(word_list):
    return [word for word in word_list if word not in custom_stopwords] if word_list else []

remove_stopwords_udf = udf(remove_stopwords, ArrayType(StringType()))

df_cleaned = df.withColumn("cleaned_text", lower(regexp_replace(col("Industry"), "[^a-zA-Z\\s]", "")))
df_tokenized = df_cleaned.withColumn("words", split(trim(col("cleaned_text")), "\\s+"))
df_filtered = df_tokenized.withColumn("filtered_words", remove_stopwords_udf(col("words")))
df_exploded = df_filtered.select(explode(col("filtered_words")).alias("word"))
df_word_counts = df_exploded.groupBy("word").agg(count("*").alias("count"))
df_frequent_words = df_word_counts.filter(col("count") >= 5)

# Industry matching
frequent_words_list = df_frequent_words.select("word").rdd.flatMap(lambda x: x).collect()

def match_industry(text):
    return next((word for word in frequent_words_list if word in text), None) if text else None

match_industry_udf = udf(match_industry, StringType())
df_final = df.withColumn("matched_industry", match_industry_udf(col("Industry")))

# Capitalize columns and fill nulls with 'Unknown'
df_final = df_final.fillna('Unknown')

def clean_and_capitalize(df, column_names):
    for column_name in column_names:
        df = df.withColumn(column_name, regexp_replace(column_name, '[^a-zA-Z\s]', '')) \
               .withColumn(column_name, initcap(column_name))
    return df

columns_to_clean = ['Full name', 'Job title', 'Sub Role', 'Company Name', 'Locality', 'Region', 'Skills', 'Gender']
df_final = clean_and_capitalize(df_final, columns_to_clean)

# Clean 'Full name': if length is greater than 34, set to 'Unknown'
df_final = df_final.withColumn('Full name', when(length(col('Full name')) > 34, 'Unknown').otherwise(col('Full name')))

# Email & Phone validation
def validate_email(email):
    return email if email and re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) else "unknown"

validate_email_udf = udf(validate_email, StringType())

df_final = df_final.withColumn("Email", validate_email_udf(col("Emails")))


# Clean 'Company Size' and 'Company Founded'
df_final = df_final.withColumn(
    'Company Size',
    when(col('Company Size').rlike('^[0-9]+$') | col('Company Size').rlike('^[0-9]+[-+][0-9]*$'), col('Company Size')).otherwise('Unknown')
)
df_final = df_final.withColumn(
    'Company Founded',
    when(col('Company Founded').rlike('^[0-9]{4}$'), col('Company Founded')).otherwise('Unknown')
)

# Extract keywords from Job Summary and Personal Summary
def extract_keywords(text):
    if text is None:
        return []
    text = text.lower().replace(".", "").replace(",", "").replace(":", "").replace(";", "")
    words = text.split()
    filtered_words = [word for word in words if word not in custom_stopwords]
    word_counts = {word: filtered_words.count(word) for word in set(filtered_words)}
    return sorted(word_counts, key=word_counts.get, reverse=True)[:3]

extract_keywords_udf = udf(extract_keywords, ArrayType(StringType()))

df_final = df_final.withColumn("job_sum_key_word", extract_keywords_udf(col("Job Summary")))
df_final = df_final.withColumn("personal_sum_keyword", extract_keywords_udf(col("Summary")))

# Clean 'Job title': if length is greater than 40, set to 'Unknown'
df_final = df_final.withColumn('Job title', when(length(col('Job title')) > 40, 'Unknown').otherwise(col('Job title')))

# Convert ARRAY<STRING> columns to a plain string
df_final = df_final.withColumn("job_sum_key_word", concat_ws(",", col("job_sum_key_word"))) \
                   .withColumn("personal_sum_keyword", concat_ws(",", col("personal_sum_keyword")))

# Write to Excel
df_pandas = df_final.toPandas()
df_pandas.to_excel('/content/uk_cleaned.xlsx', index=False, engine='openpyxl')

# Show final table
df_final.show(truncate=False)

# Drop used columns
columns_to_drop2 = ['Industry', 'Location', 'First Name', 'Last Name', 'Company Location Name', 'Last Updated46', 'Last Updated55']
df_final = df_final.drop(*columns_to_drop2)

