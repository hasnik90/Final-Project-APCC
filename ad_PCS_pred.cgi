#!/usr/local/bin/python3

import jinja2
import mysql.connector as mysql
from mysql.connector import Error
import cgi
import pandas as pd
import csv

#form = cgi.FieldStorage()

#file1= form.getfirst("file1")
#file2 = form.getfirst("file2")


#This line tells the template loader where to search for template files
templateLoader = jinja2.FileSystemLoader( searchpath="/var/www/html/hkannan2/final/templates")

#This creates your environment and loads a specific template
env = jinja2.Environment(loader=templateLoader)
template = env.get_template('final.html')

#create the cursor object, If the tables exist remove all of them prior to creating tables for new sample files.Create two tables each with the same column number, names and data types for the 2 sample input files
try:
  conn = mysql.connect(user='hkannan2',password='Ihatemysql1990!',host='localhost',database='hkannan2')
  if conn.is_connected():
    curs = conn.cursor()
    curs.execute('DROP TABLE IF EXISTS Annotated_SNP_1;')
    curs.execute('DROP TABLE IF EXISTS Annotated_SNP_2;')
    curs.execute('DROP TABLE IF EXISTS filtered_SNP_1;')
    curs.execute('DROP TABLE IF EXISTS filtered_SNP_2;')
    qry_1 = "CREATE TABLE Annotated_SNP_1(SNP_ID varchar(255) NOT NULL, Allele varchar(255), Consequence varchar(255), Impact varchar(10), Symbol varchar(255), Gene varchar(255), Feature_type varchar(255),Feature varchar(255), Biotype varchar(255),HGVSc varchar(255),HGVSp varchar(255), cDNA_position varchar(255), CDS_position varchar(255), Protein_position varchar(255), Amino_acids varchar(10), Codons varchar(100), Existing_variation varchar(255), Distance int, Strand varchar(10), Flags varchar(255), Symbol_source varchar(100), HGNC_ID varchar(100), Canonical varchar(10), SIFT varchar(255), Polyphen varchar(255), Domains varchar(255), genomAD_AF double,genomAD_filtered varchar(10), CLIN_SIG varchar(255), Phenotypes varchar(255),Condel varchar(255), Ancestral_allele varchar(10),Clinvar_clinsig varchar(100), Clinvar_rs varchar(255), NF_filter varchar(100), Depth int, HOMO varchar(10), HETERO varchar(10))"      
    qry_2 = "CREATE TABLE Annotated_SNP_2(SNP_ID varchar(255) NOT NULL, Allele varchar(255), Consequence varchar(255), Impact varchar(10), Symbol varchar(255), Gene varchar(255), Feature_type varchar(255),Feature varchar(255), Biotype varchar(255),HGVSc varchar(255),HGVSp varchar(255), cDNA_position varchar(255), CDS_position varchar(255), Protein_position varchar(255), Amino_acids varchar(10), Codons varchar(100), Existing_variation varchar(255), Distance int, Strand varchar(10), Flags varchar(255), Symbol_source varchar(100), HGNC_ID varchar(100), Canonical varchar(10), SIFT varchar(255), Polyphen varchar(255), Domains varchar(255), genomAD_AF double,genomAD_filtered varchar(10), CLIN_SIG varchar(255), Phenotypes varchar(255),Condel varchar(255), Ancestral_allele varchar(10),Clinvar_clinsig varchar(100), Clinvar_rs varchar(255), NF_filter varchar(100), Depth int, HOMO varchar(10), HETERO varchar(10))" 
    curs.execute(qry_1)
    curs.execute(qry_2)
    
    #Read the sample input tab-delimited files using pandas read_csv and add the table contents to pandas dataframes
    df_1 = pd.read_csv("Test_011.txt", sep="\t")
    df_1 = df_1.where((pd.notnull(df_1)), None)

    df_2 = pd.read_csv("Test2.txt", sep="\t")
    df_2 = df_2.where((pd.notnull(df_2)), None)
   
    #Iterate through each dataframe and insert the content row by row to the created sql tables
    for i,row in df_1.iterrows():
       sql = "INSERT INTO hkannan2.Annotated_SNP_1 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
       curs.execute(sql,tuple(row))
       conn.commit()
    for i,row in df_2.iterrows():
       sql = "INSERT INTO hkannan2.Annotated_SNP_2 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
       curs.execute(sql,tuple(row))
       conn.commit()
    
    #Filter through the first sql files containing the information from the input file 1 and insert the output to a new table - filtered_SNP_1
    filter_qry_1 = " CREATE TABLE filtered_SNP_1 AS SELECT * FROM Annotated_SNP_1 WHERE CLIN_SIG LIKE %s AND Strand LIKE %s AND Canonical LIKE %s AND genomAD_filtered IN (%s,%s) UNION ALL SELECT * FROM Annotated_SNP_1 WHERE NOT CLIN_SIG LIKE %s AND Strand LIKE %s AND Canonical LIKE %s AND Depth >=  %s AND HETERO LIKE %s AND genomAD_filtered IN (%s,%s) AND Impact LIKE %s"
    curs.execute(filter_qry_1, ("%"+ "pathogenic"+ "%","1","YES","YES","UNKNOWN","%"+ "pathogenic"+ "%","1","YES",8,'1',"YES","UNKNOWN","HIGH"))


#Filter through the second sql files containing the information from the input file 2 and insert the output to a new table - filtered_SNP_2
    filter_qry_2 = "CREATE TABLE filtered_SNP_2 AS SELECT * FROM Annotated_SNP_2 WHERE Annotated_SNP_2.CLIN_SIG LIKE %s AND Annotated_SNP_2.Strand LIKE %s AND Annotated_SNP_2.Canonical LIKE %s AND Annotated_SNP_2.genomAD_filtered IN (%s,%s) UNION ALL SELECT * FROM Annotated_SNP_2 WHERE NOT Annotated_SNP_2.CLIN_SIG LIKE %s AND Annotated_SNP_2.Strand LIKE %s AND Annotated_SNP_2.Canonical LIKE %s AND Annotated_SNP_2.Depth >=  %s AND Annotated_SNP_2.HETERO LIKE %s AND Annotated_SNP_2.genomAD_filtered IN (%s,%s) AND Annotated_SNP_2.Impact LIKE %s"
    curs.execute(filter_qry_2, ("%"+"pathogenic"+ "%","1","YES","YES","UNKNOWN","%"+ "pathogenic"+ "%","1","YES",8,'1',"YES","UNKNOWN","HIGH"))

    #Identify the rows with the same SNP_ID and Zygocity in the two filtered tables
    filter_qry_3 = "SELECT * FROM filtered_SNP_1 WHERE EXISTS(SELECT * FROM filtered_SNP_2 WHERE filtered_SNP_1.SNP_ID = filtered_SNP_2.SNP_ID AND filtered_SNP_1.HETERO=filtered_SNP_2.HETERO)"
    curs.execute(filter_qry_3)

except Error as e:
   print("Error while connecting to MySQL", e)


#Iterate through the final matching output from the filtered tables stored in curs object. Each row is dictionary with key-value pairs. Keys are same as the columns in the sql table and each value in a rwo is attached to a key.After each iteration the dictionary is appeneded to a list of dictionaries.This list is sent to the final html file tto create tthe output table. 
dicts_list = [{}]
for row in curs:
       dicts = {"SNP_ID":[],"Allele":[], "Consequence":[],"Impact":[],"Symbol":[],"Gene":[],"Feature_type":[],"Feature":[],"Biotype":[],"HGVSc":[],"HGVSp":[],"cDNA_position":[],"CDS_position":[],"Protein_position":[],"Amino_acids":[],"Codons":[],"Existing_variation":[],"Distance":[],"Strand":[],"Flags":[],"Symbol_source":[],"HGNC_ID":[],"Canonical":[],"SIFT":[],"Polyphen":[],"Domains":[],"genomAD_AF":[],"genomAD_filtered":[],"CLIN_SIG":[],"Phenotypes":[],"Condel":[],"Ancestral_allele":[],"Clinvar_clinsig":[],"Clinvar_rs":[],"NF_filter":[],"Depth":[],"HOMO":[],"HETERO":[]}
       dicts["SNP_ID"]   = row[0]
       dicts["Allele"]   = row[1]
       dicts["Consequence"]= row[2]
       dicts["Impact"] = row[3]
       dicts["Symbol"] = row[4]
       dicts["Gene"]= row[5]
       dicts["Feature_type"] = row[6]
       dicts["Feature"] = row[7]
       dicts["Biotype"] = row[8]
       dicts["HGVSc"] = row[9]
       dicts["HGVSp"] = row[10]
       dicts["cDNA_position"] = row[11]
       dicts["CDS_position"] = row[12]
       dicts["Protein_position"] = row[13]
       dicts["Amino_acids"] = row[14]
       dicts["Codons"] = row[15]
       dicts["Existing_variation"] = row[16]
       dicts["Distance"] = row[17]
       dicts["Strand"] = row[18]
       dicts["Flags"] = row[19]
       dicts["Symbol_source"] = row[20]
       dicts["HGNC_ID"] = row[21]
       dicts["Canonical"] = row[22]
       dicts["SIFT"] = row[23]
       dicts["Polyphen"] = row[24]
       dicts["Domains"] = row[25]
       dicts["genomAD_AF"] = row[26]
       dicts["genomAD_filtered"] = row[27]
       dicts["CLIN_SIG"] = row[28]
       dicts["Phenotypes"] = row[29]
       dicts["Condel"] = row[30]
       dicts["Ancestral_allele"] = row[31]
       dicts["Clinvar_clinsig"] = row[32]
       dicts["Clinvar_rs"] = row[33]
       dicts["NF_filter"] = row[34]
       dicts["Depth"] = row[35]
       dicts["HOMO"] = row[36]
       dicts["HETERO"] = row[37]
       dicts_list.append(dicts)


print("Content-Type: text/html\n\n")
print(template.render(List = dicts_list))
