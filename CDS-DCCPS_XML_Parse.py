import xml.etree.ElementTree as ET
import pandas as pd
import argparse
import argcomplete

parser = argparse.ArgumentParser(
                    prog='CCDI_CDS_Pipeline.py',
                    description='A script to run a pipeline for either a project that is CCDI only, CDS only or from CCDI to CDS.',
                    )

parser.add_argument( '-s', '--submission', help='The input submission XML file')
parser.add_argument( '-e', '--experiment', help="The input experiment XML file")
parser.add_argument( '-r', '--run', help='The input run XML file')

argcomplete.autocomplete(parser)

args = parser.parse_args()

#pull in args as variables
submission=args.submission
experiment=args.experiment
run=args.run


##################
#
# Submission XML
#
##################

#Setting up the experiment data frame 
cols = ["TITLE","PHS_ID","CENTER_NAME", "CONTACT_NAME", "CONTACT_EMAIL"]
rows = []
  
# Parsing the experiment XML file
tree= ET.parse(submission)
root = tree.getroot()

#Walk through tree to pull out exp portions
PHS_ID=root.attrib['alias']
CENTER_NAME=root.attrib['center_name']
TITLE=root.find('TITLE').text
for cont in root.findall('CONTACTS'):
    CONTACT_NAME=cont.find("CONTACT").attrib['name']
    CONTACT_EMAIL=cont.find("CONTACT").attrib['inform_on_status']

    #Append the experiment data per row
    rows.append({"TITLE": TITLE,
                "PHS_ID": PHS_ID,
                "CENTER_NAME": CENTER_NAME,
                "CONTACT_NAME": CONTACT_NAME,
                "CONTACT_EMAIL": CONTACT_EMAIL})
  
df_subm = pd.DataFrame(rows, columns=cols)
  
# Writing experiment dataframe to csv
df_subm.to_csv('submission_output.csv', index=False)


##################
#
# Experiment XML
#
##################

#Setting up the experiment data frame 
cols = ["SAMPLE_ID","PHS_ID", "LIBRARY_NAME", "LIBRARY_STRATEGY", "LIBRARY_SOURCE", "LIBRARY_SELECTION", "LIBRARY_LAYOUT","INSTRUMENT_MODEL", "DESIGN_DESCRIPTION"]
rows = []
  
# Parsing the experiment XML file
tree= ET.parse(experiment)
root = tree.getroot()

#Walk through tree to pull out exp portions
for exp in root.findall('EXPERIMENT'):
    TITLE=exp.find('TITLE').text
    for des in exp.findall('DESIGN'):
        DESIGN_DESCRIPTION = des.find("DESIGN_DESCRIPTION").text
        SAMPLE_ID = des.find("SAMPLE_DESCRIPTOR").attrib['refname']
        PHS_ID= des.find("SAMPLE_DESCRIPTOR").attrib['refcenter']
        for lib_desc in des.findall('LIBRARY_DESCRIPTOR'):
            LIBRARY_NAME = lib_desc.find("LIBRARY_NAME").text
            LIBRARY_STRATEGY = lib_desc.find("LIBRARY_STRATEGY").text
            LIBRARY_SOURCE = lib_desc.find("LIBRARY_SOURCE").text
            LIBRARY_SELECTION = lib_desc.find("LIBRARY_SELECTION").text
            for lib_lay in lib_desc.findall("LIBRARY_LAYOUT"):
                for lib in lib_lay:
                    LIBRARY_LAYOUT = lib.tag
    for plat in exp.findall("PLATFORM"):
        for ill in plat.findall("ILLUMINA"):
            INSTRUMENT_MODEL= ill.find("INSTRUMENT_MODEL").text
                
#Append the experiment data per row
    rows.append({"DESIGN_DESCRIPTION": DESIGN_DESCRIPTION,
                "SAMPLE_ID": SAMPLE_ID,
                "PHS_ID": PHS_ID,
                "LIBRARY_NAME": LIBRARY_NAME,
                "LIBRARY_STRATEGY": LIBRARY_STRATEGY,
                "LIBRARY_SOURCE": LIBRARY_SOURCE,
                "LIBRARY_SELECTION": LIBRARY_SELECTION,
                "LIBRARY_LAYOUT": LIBRARY_LAYOUT,
                "INSTRUMENT_MODEL": INSTRUMENT_MODEL})
  
df_exp = pd.DataFrame(rows, columns=cols)
  
# Writing experiment dataframe to csv
df_exp.to_csv('experiment_output.csv', index=False)


##################
#
# Run XML
#
##################


#Setting up the run data frame 
cols = ["LIBRARY_NAME", "ASSEMBLY", "ACTIVE_URL", "BASES", "READS", "COVERAGE", "AVG_READ_LENGTH", "CHECKSUM", "CHECKSUM_METHOD", "FILENAME", "FILETYPE", "ALIAS"]
rows = []
  
# Parsing the run XML file
tree= ET.parse(run)
root = tree.getroot()

#Walk through tree to pull out run_set portions
for run in root.findall('RUN'):
    ALIAS=run.attrib['alias']
    LIBRARY_NAME=run.find("EXPERIMENT_REF").attrib['refname']
    RUN_ATTRIBUTES=[]
    for run_att in run.find("RUN_ATTRIBUTES").findall("RUN_ATTRIBUTE"):
        RUN_ATTRIBUTES.append({run_att.find("TAG").text:run_att.find("VALUE").text})

    #Break list of dicts into one dictionary
    RUN_ATTRIBUTES_clean={}
    for attr in RUN_ATTRIBUTES:
        RUN_ATTRIBUTES_clean.update(attr)

    ASSEMBLY=RUN_ATTRIBUTES_clean["assembly"]
    ACTIVE_URL=RUN_ATTRIBUTES_clean["active_location_URL"]
    BASES=RUN_ATTRIBUTES_clean["Bases"]
    READS=RUN_ATTRIBUTES_clean["Reads"]
    COVERAGE=RUN_ATTRIBUTES_clean["coverage"]
    AVG_READ_LENGTH=RUN_ATTRIBUTES_clean["AvgReadLength"]
    
    for file in run.find("DATA_BLOCK").find("FILES").findall("FILE"):
        CHECKSUM=file.attrib['checksum']
        CHECKSUM_METHOD=file.attrib['checksum_method']
        FILENAME=file.attrib['filename']
        FILETYPE=file.attrib['filetype']

#Append the run data per row
        rows.append({"LIBRARY_NAME": LIBRARY_NAME,
                    "ASSEMBLY": ASSEMBLY,
                    "ACTIVE_URL": ACTIVE_URL,
                    "BASES": BASES,
                    "READS": READS,
                    "COVERAGE": COVERAGE,
                    "AVG_READ_LENGTH": AVG_READ_LENGTH,
                    "CHECKSUM": CHECKSUM,
                    "CHECKSUM_METHOD": CHECKSUM_METHOD,
                    "FILENAME": FILENAME,
                    "FILETYPE": FILETYPE,
                    "ALIAS": ALIAS})

  
df_run = pd.DataFrame(rows, columns=cols)
  
# Writing experiment dataframe to csv
df_run.to_csv('run_output.csv', index=False)
