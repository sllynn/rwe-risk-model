import typing
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

class RWDDataset:
  def __init__(self, db_name: str, delta_root_path: str, spark: SparkSession, dbutils):
    self.db_name = db_name
    self.delta_root_path = delta_root_path
    self._pii_col_names = []
    self.spark = spark
    self.dbutils = dbutils
    
  def refresh(self, ehr_path: str):
    ehr_dfs = self.read_data(ehr_path)
    self.drop_all_tables()
    self.clear_delta_path()
    self.create_database()
    self.encounters = (
      ehr_dfs['encounters']
      .withColumnRenamed('Id','Enc_Id')
      .withColumnRenamed('START', 'START_TIME')
      .withColumnRenamed('END', 'END_TIME')
      .withColumnRenamed('PROVIDER', 'ORGANIZATION')
    )
    self.providers = (
      ehr_dfs['providers']
      .withColumnRenamed('NAME','Provider_Name')
      .withColumnRenamed('Id','PROVIDER')
    )
    self.organizations = (
      ehr_dfs['organizations']
      .withColumnRenamed('NAME', 'Org_Name')
      .withColumnRenamed('Id', 'ORGANIZATION')
      .withColumnRenamed('ADDRESS', 'PROVIDER_ADDRESS')
      .withColumnRenamed('CITY', 'PROVIDER_CITY')
      .withColumnRenamed('STATE', 'PROVIDER_STATE')
      .withColumnRenamed('ZIP', 'PROVIDER_ZIP')
      .withColumnRenamed('GENDER', 'PROVIDER_GENDER')
    )
    self.patients = (
      self.obfuscate_data(ehr_dfs["patients"])
      .withColumnRenamed('Id', 'PATIENT')
    )
    self.patient_encounters = (
      self.encounters
      .join(self.patients, ['PATIENT'])
      .join(self.organizations, ['ORGANIZATION'])
    )
  
  def read_data(self, input_path: str) -> typing.Dict[str, DataFrame]:
    df_dict = {}
    for path,name in [(f.path,f.name) for f in self.dbutils.fs.ls(input_path) if f.name !='README.txt']:  
      df_name = name.replace('.csv','')
      df_dict[df_name] = self.spark.read.csv(path, header=True, inferSchema=True)
    return df_dict
  
  @property
  def patients(self) -> DataFrame:
    return self.spark.table("patients")
  
  @patients.setter
  def patients(self, df: DataFrame):
    self.save_to_table(df, "patients")
    
  @property
  def organizations(self) -> DataFrame:
    return self.spark.table("organizations")
  
  @organizations.setter
  def organizations(self, df: DataFrame):
    self.save_to_table(df, "organizations")
    
  @property
  def providers(self) -> DataFrame:
    return self.spark.table("providers")
  
  @providers.setter
  def providers(self, df: DataFrame):
    self.save_to_table(df, "providers")
    
  @property
  def encounters(self) -> DataFrame:
    return self.spark.table("encounters")
  
  @encounters.setter
  def encounters(self, df: DataFrame):
    self.save_to_table(df, "encounters")
    
  @property
  def patient_encounters(self) -> DataFrame:
    return self.spark.table("patient_encounters")
  
  @patient_encounters.setter
  def patient_encounters(self, df: DataFrame):
    self.save_to_table(df, "patient_encounters")

  @property
  def pii_col_names(self):
    return self._pii_col_names
  
  @pii_col_names.setter
  def pii_col_names(self, pii_cols: typing.List[str]):
    self._pii_col_names = pii_cols
      
  def obfuscate_data(self, sensitive_data: DataFrame) -> DataFrame:
    for c in self.pii_col_names:
      sensitive_data = sensitive_data.withColumn(c, sha1(c))
    return sensitive_data
  
  def drop_all_tables(self):
    self.spark.sql(f"drop database if exists {self.db_name} cascade")
  
  def clear_delta_path(self):
    self.dbutils.fs.rm(self.delta_root_path, True)
    
  def create_database(self):
    self.spark.sql(f"create database {self.db_name} comment 'Database for real world data' location '{self.delta_root_path}/database'")
    self.spark.sql(f"use {self.db_name}")
    
  def save_to_table(self, df: DataFrame, entity: str):
    df.write.mode("overwrite").saveAsTable(name=entity, format="delta", path=f"{self.delta_root_path}/{entity}")
  