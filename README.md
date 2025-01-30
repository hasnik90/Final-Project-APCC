This program generates potential pathogenic variant/s from Next Generation Sequencing(NGS) genomic data based on a published filteration method.

The input file has to be in tab separated values(tsv) format, converted from a GATK output variant call format(vcf) file annotated with ANNOVAR, VEP, SnpEff or similar tool with mandatory fields as described below, prior to using this tool.
HTML/CSS form is presented to the user for the input tsv file. Once the file is submitted, an SQL table is created in the MySQL server, and python-based CGI script importes content of the file in to the table. A pre-established filter startegy in the form of 
SQL queries in the CGI script is performed the quesries on the newly imported table. The resulting output or the potential pathogenic variant/s from the input file, is then transferred as a data structure such as list of dictionaries to a HTML/CSS form for user 
visualization. 

The credential for Mysql server in PCS_pred.cgi are currently not in use. User must add their own credentials to their own MySQL server.

The mandatory fields in the tsv input that are filtered in the tool

1. Consequence - type of variant

![Picture1](https://github.com/user-attachments/assets/500034b5-a85c-4c71-8262-673c7593d661)



2. Impact – High/ Moderate/Low/Modifier
3. Strand
4. Canonical
5. genomeAD allele frequency
6. Clinical Significance

<img width="239" alt="Picture2" src="https://github.com/user-attachments/assets/0e57dfe4-b7a0-4da7-b1b3-579355becede" />

7. depth
8. Heterozygocity/Homozygocity


