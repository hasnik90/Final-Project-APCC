#!/usr/local/bin/python3

import jinja2
import mysql.connector as mysql
from mysql.connector import Error
import cgi
import pandas as pd
import csv

form = cgi.FieldStorage()
file1= form.getfirst("single_file")

#This line tells the template loader where to search for template files
templateLoader = jinja2.FileSystemLoader( searchpath="/var/www/html/hkannan2/final/templates")

#This creates your environment and loads a specific template
env = jinja2.Environment(loader=templateLoader)
template = env.get_template('final.html')

#add your own credentials to a MySQL server
#create the cursor object, If the tables exist remove all of them prior to creating tables for new sample files.Create a table with  column number, names and data types as below for the  input file
try:
  conn = mysql.connect(user='hkannan2',password='Ihatemysql1990!',host='localhost',database='hkannan2')
  if conn.is_connected():
    curs = conn.cursor()
    curs.execute('DROP TABLE IF EXISTS Annotated_SNP_1;')
    
    qry_1 = "CREATE TABLE Annotated_SNP_1(SNP_ID varchar(255) NOT NULL, Allele varchar(255), Consequence varchar(255), Impact varchar(10), Symbol varchar(255), Gene varchar(255), Feature_type varchar(255),Feature varchar(255), Biotype varchar(255),HGVSc varchar(255),HGVSp varchar(255), cDNA_position varchar(255), CDS_position varchar(255), Protein_position varchar(255), Amino_acids varchar(10), Codons varchar(100), Existing_variation varchar(255), Distance int, Strand varchar(10), Flags varchar(255), Symbol_source varchar(100), HGNC_ID varchar(100), Canonical varchar(10), SIFT varchar(255), Polyphen varchar(255), Domains varchar(255), genomAD_AF double,genomAD_filtered varchar(10), CLIN_SIG varchar(255), Phenotypes varchar(255),Condel varchar(255), Ancestral_allele varchar(10),Clinvar_clinsig varchar(100), Clinvar_rs varchar(255), NF_filter varchar(100), Depth int, HOMO varchar(10), HETERO varchar(10))"      
    curs.execute(qry_1)
   
    
    #Read the sample input tab-delimited file using pandas read_csv and add the table contents to pandas dataframe
    df = pd.read_csv(file1, sep="\t")
    df = df.where((pd.notnull(df)), None)

   
    #Iterate through the dataframe and insert the content row by row to the created sql table
    for i,row in df.iterrows():
       sql = "INSERT INTO hkannan2.Annotated_SNP_1 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
       curs.execute(sql,tuple(row))
       conn.commit()
    
    #Filter through the sql file containing the information from the input file
    filter_qry = " SELECT * FROM Annotated_SNP_1 WHERE CLIN_SIG LIKE %s AND Strand LIKE %s AND Canonical LIKE %s AND genomAD_filtered IN (%s,%s) UNION ALL SELECT * FROM Annotated_SNP_1 WHERE NOT CLIN_SIG LIKE %s AND Strand LIKE %s AND Canonical LIKE %s AND Depth >=  %s AND HETERO LIKE %s AND genomAD_filtered IN (%s,%s) AND Impact LIKE %s"
    curs.execute(filter_qry, ("%"+ "pathogenic"+ "%","1","YES","YES","UNKNOWN","%"+ "pathogenic"+ "%","1","YES",8,'1',"YES","UNKNOWN","HIGH"))

except Error as e:
   print("Error while connecting to MySQL", e)


#Iterate through the final output from the filtered table stored in curs object. Each row is dictionary with key-value pairs. Keys are same as the columns in the sql table and each value in a rwo is attached to a key.After each iteration the dictionary is appeneded to a list of dictionaries.This list is sent to the final html file tto create tthe output table. 
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
