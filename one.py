import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import *
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,['bucket','file'])
bucket = args['bucket']
obj = args['file']
print("yash")

data = "s3a://{}/{}".format(bucket,obj)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

def extract_function(obj):
    start_index = 0
    end_index = obj.find('.')
    tab = obj[start_index:end_index]
    return tab

tab = extract_function(obj)
Redshift_table ="public.{}".format(tab)
table_2 = "CALL final.{}();".format(tab)

# Configure Redshift credentials
redshift_url = "jdbc:redshift://redshift-cluster-2.codt3zpdvhgs.ap-south-1.redshift.amazonaws.com:5439/dev"
redshift_user = "awsuser"
redshift_password = "mypassword1A"
redshift_region = "ap-south-1"
redshift_role_arn = "arn:aws:iam::267797528699:role/demo-redshift"

# Configure AWS credentials
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# Read data from S3
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load(data)

# Write data to Redshift
df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", Redshift_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("aws_iam_role", redshift_role_arn) \
    .option("aws_region", redshift_region) \
    .mode("append") \
    .save()

# Call a stored procedure in Redshift
def call_stored_procedure():
    # Create a Redshift client
    client = boto3.client('redshift-data', region_name=redshift_region)
   
    # Call the stored procedure
    sql = table_2
    response = client.execute_statement(
        ClusterIdentifier='redshift-cluster-2',
        Database='dev',
        DbUser='awsuser',
        Sql=sql
    )
   
    # Print the response
    print(response)

# Call the stored procedure
call_stored_procedure()

# Stop the SparkSession
spark.stop()
