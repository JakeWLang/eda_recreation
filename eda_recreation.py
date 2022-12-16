#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 20:51:22 2022

@author: jakelang
"""

# Welcome to the script! Following imports, I have a sereis of complementary
# functions that help build out the tools we need before beginning with the
# actual recreation of EDAs. I define each function and lead readers and,
# hopefully, users to better use and manipulate the code in the future.

# Not every gap is filled - purposely. There aren't many NAs (-666666)
# for the CMAP region when recreating EDAs but they're still there. Different
# users may want to do different things with those NA-values. In fact, to list
# the options: we could fill them with the median income for the MSA as a whole
# (which I do include as an option!), fill them with
# zeros (or treat them as zeros), or drop them altogether, but that's careless
# and we like data. I'm sure there are other options, feel free to explore them.
# I hope this script is accessible and don't hestitate to reach out with
# any questions if you have any. My email is jacobwlang@gmail.com


from census import Census
import pandas as pd
import geopandas as gpd
import numpy as np
import us
from us import states


# Enter your own census api key. Don't have one? Get it here:
# https://api.census.gov/data/key_signup.html
CENSUS_KEY = r'e6a46fdf52e4035553df66415ff958e025dcd5e6'

WD = '/Users/jakelang/Documents/Harris - MPP - 2021-2022/Winter 2022/Policy Lab 22/CMAP EDA Recreation'

# Loading the census api key into the census api wrapper
c = Census(CENSUS_KEY)



# Defines a function that can help search through the census tables.
# If you know a term, throw it in and see what turns up of all available census
# vars.
def find_vars(census_var_table, search_string, get_tab):
    n = 0
    possible_loc = []
    for desc in census_var_table['description']:
        n += 1
        if search_string in desc.lower():
            possible_loc.append(n - 1)
            
    if get_tab:
        return(census_var_table.iloc[possible_loc])
    else:
        return(possible_loc)
    

# Defines a function that finds relevant variable numbers from a list of 
# numbers based on the table of interest. In the case of native language,
# tab = B16008 and nums = vars of interest from that table
def get_var_nums(tab, nums):
    fin_num_list = ['{}_{}E'.format(tab,num) for num in nums]
    return fin_num_list


# Defines a function that digs through ACS data based on states of interest
# and returns a combined file with tract-level information
def gen_table_county_tract_acs(variables, states_of_interest, acs1_or5, c = c):
    if type(states_of_interest) != list:
        states_of_interest = [states_of_interest]

    # If not using full list of us states, turn each listed state into a fips
    # code. Else, take those us.states.State objects and turn them into fips
    # codes. This code introduces flexibility by state-selection OR full-country
    # selection

    if type(states_of_interest[0]) != us.states.State:
        print("You're only using your own list of states!")
        state_fips = [states.lookup(
            state).fips for state in states_of_interest]
    else:
        print("You're using all the states")
        state_fips = [state.fips for state in states_of_interest]

    starting_frame = pd.DataFrame()
    for each_state in state_fips:
        print('starting the df creation')
        # Creates a dataframe from the redistricting data for each state
        # and each variable listed in the arguments
        if acs1_or5 == 'acs5':
            state_df = pd.DataFrame(c.acs5.state_county_tract(variables, each_state,
                                                              Census.ALL, Census.ALL))
        elif acs1_or5 == 'acs1':
            state_df = pd.DataFrame(c.acs1.state_county_tract(variables, each_state,
                                                              Census.ALL, Census.ALL))
            
        starting_frame = pd.concat([starting_frame, state_df])

        geoid_frame = clean_state_geoid(starting_frame)

    return(geoid_frame)


# Defines a function that applies 0s to state, country, and tract codes missing
# (e.g. state 1 should be state 01, county 53 must be 053, etc.). Basically just
# pads as necessary. This is a bastardization of rjust before I knew rjust existed
def add_zero(entry, state_co_tract):
    entry = str(entry)
    if state_co_tract == 'state':
        if (len(entry) == 1) & (entry[0] != '0'):
            new_entry = '0' + entry
            return(new_entry)
        else:
            return(entry)

    elif state_co_tract == 'county':
        if len(entry) == 1:
            new_entry = '00' + entry
            return(new_entry)
        elif len(entry) == 2:
            new_entry = '0' + entry
            return(new_entry)
        else:
            return(entry)
    elif state_co_tract == 'tract':
        if len(entry) == 3:
            new_entry = '000' + entry
            return(new_entry)
        elif len(entry) == 4:
            new_entry = '00' + entry
            return(new_entry)
        elif len(entry) == 5:
            new_entry = '0' + entry
            return(new_entry)
        else:
            return(entry)
    else:
        return(entry)


# Defines GEOID based on state county tract specification for spatial analysis
# and linking
def make_geoid(df):
    df['GEOID'] = df['state'].astype(
        str) + df['county'].astype(str) + df['tract'].astype(str)

    return(df)

# Applies zeroes as the above add_zero function describes
def clean_state_geoid(df):
    df['state'] = df['state'].astype(str)
    df['state'] = df['state'].apply(lambda x: add_zero(x, 'state'))
    df['county'] = df['county'].astype(str)
    df['county'] = df['county'].apply(lambda x: add_zero(x, 'county'))
    df['tract'] = df['tract'].astype(str)
    df['tract'] = df['tract'].apply(lambda x: add_zero(x, 'tract'))
    df = make_geoid(df)
    return(df)



# Defines a function that flags census tracts as EDAs per CMAP methodology
def eda_flag(df, msa_name_col, total_race_col, white_race_col, lep_cols,
             total_lep_col, med_inc_vars, no_na = False):
    wdf = df
    msas = list(df[msa_name_col].unique())
    
    # Establish a new column that is poc, subtracting white from total
    # then dividing that non_white col into .
    wdf['perc_poc'] = ( (wdf[total_race_col] - wdf[white_race_col]) /
                        wdf[total_race_col] )
    
    # Summing across axis to get total lep and taking lep per tract as percent.
    wdf['lep_sum'] = wdf[lep_cols].sum(axis = 1)
    wdf['perc_lep'] = wdf['lep_sum']/wdf[total_lep_col]
    
    # Defining num households under median income by cutting household num in half
    for var in household_vars:
        df['half_{}'.format(var)] = round(df[var]/2)
                
    
    # Establishes an empty df with column names from the base df
    additive_df = pd.DataFrame(columns = wdf.columns)
    
    # Basically, everything that needs to happen happens in this loop
    for msa in msas:
        msa_df = wdf[wdf[msa_name_col] == msa].reset_index()
        
        # Establish total poc avg. across all tracts in an MSA
        tot_poc_avg = msa_df['perc_poc'].mean()
        tot_lep_avg = msa_df['perc_lep'].mean()
        
        
        # Flag race
        mas_df['race_flag'] = msa_df['perc_poc'].apply(lambda x: 1 if x >= tot_poc_avg else 0)
        # Flag lep
        mas_df['lep_flag'] = msa_df['perc_lep'].apply(lambda x: 1 if x >= tot_lep_avg else 0)

        
        # Flag med_inc
        
        # Create a total count for households
        msa_df['tot_families'] = msa_df[household_vars].sum(axis = 1)
        
        # Find 60% median income for each household size variable:
        for i in range(len(med_inc_vars)):
            # Define median income 60% thresh and then flag against it using
            # function
            
            # If clean, clean out all NAs (major data loss).
            if no_na == 'clean':
                # filter out <0s
                msa_df = msa_df[msa_df[med_inc_vars[i]] > 0]
                msa_df['{}_60'.format(med_inc_vars[i])] = msa_df[med_inc_vars[i]].median() * 0.6
                msa_df = msa_df.drop('index', axis = 1)
                msa_df = msa_df.reset_index()
            
            # If replace, replace missing threshold values with the MSA's overall MHI.
            elif no_na == 'replace':
                if msa_df[med_inc_vars[i]].median() < 0:
                    msa_df['{}_60'.format(med_inc_vars[i])] = msa_df['B19113_001E'].median() * 0.6
                else:
                    msa_df['{}_60'.format(med_inc_vars[i])] = msa_df[med_inc_vars[i]].median() * 0.6

        
            else:
                msa_df['{}_60'.format(med_inc_vars[i])] = msa_df[med_inc_vars[i]].median() * 0.6
            
            msa_df[f'{med_inc_vars[i]}_flag'] = msa_df\
            .apply(lambda row: 1 if row[f'{med_inc_vars[i]}_60'] <= 0 or row[med_inc_vars[i]] <= row[f'{med_inc_vars[i]}_60']
                else 0, axis = 1)

            # If flag, count
            msa_df[f'{household_vars[i]}_count_pop'] = msa_df\
            .apply(lambda row: round(row[household_vars[i]]) if row[f'{med_inc_vars[i]}_flag'] == 1 else 0, axis = 1)
            
        # Finished with the loop up there, we start to look at the total counts
        # and add them with the final function, here.
        # Starting with varlist creation to draw count names out in the next func.
        count_vars = [house_var + '_count_pop' for house_var in household_vars]
        msa_df['pop_in_pov'] = msa_df[count_vars].sum()

        msa_df['inc_flag'] = msa_df\
        .apply(lambda row: 1 if (row['pop_in_pov'] >= row['tot_families']*.05) and (row['tot_families'] > 0) else 0, axis = 1)

        msa_df['eda_flag'] = msa_df\
        .apply(lambda row: 1 if (row['race_flag'] == 1 and row['inc_flag'] == 1)
            or (row['lep_flag'] == 1 and row['inc_flag'] == 1) else 0, axis = 1)

        additive_df = pd.concat([additive_df, msa_df])
    
    return(additive_df)

######### BEGIN CODE RUN #############

# Loading in ACS5-table, you read through this if you're looking for new vars, 
# not relevant for this but could be useful in the future :)
acs5_tab = pd.DataFrame(c.acs5.tables())

# Establishing important var lists. NOTE: these are the variables used to 
# create EDAs. In this case, I use the ACS5-Year Estimates to recreate single-family
# median income whereas the EDA Methodology document by CMAP uses the ACS1-Year

# Create variables for LEP, Household Count by Household Size, Median Income,
# and Race
lep_nums = ['001', '007','010','015','018','025','028','033','036','042','045',
            '050','053']
household_nums = ['003','004','005','006','007','008','010']
med_inc_nums = ['002','003','004','005','006','007']
race_nums = ['001', '003', '004', '005', '006', '007', '008', '009', '012']


lep_vars = get_var_nums('B16008', lep_nums)



household_vars = get_var_nums('B11016', household_nums)

med_inc_vars = get_var_nums('B19119', med_inc_nums)
# Manually appending single-family households median income since it stems
# from a different census table.
med_inc_vars.append('B19019_002E')

race_vars = get_var_nums('B03002', race_nums)

# Bringing in overall household median income to fill NAs with,
# just one of many possible options.
tot_median_income = 'B19113_001E'

# Defining the final list of eda determinants.
eda_vars = race_vars + household_vars + med_inc_vars + [tot_median_income] + lep_vars 

# Also defining keys for the function since the names are var nums.
total_race = 'B03002_001E'
white = 'B03002_003E'
tot_lep_col = 'B16008_001E'


# Generating all the variables needed for EDA flagging. Use us.states.STATES
# to get all states, otherwise, you can use ['IL', 'MI', etc.] or just 'IL'.
data_19 = gen_table_county_tract_acs(eda_vars, us.states.STATES, 'acs5')


# Bringing in joined tracts and MSAs created by a simple spatial merge
# any tract intersecting an MSA's boundary adopts the MSA's name.
join_tract_msa = gpd.read_file(WD + '/Tracts with MSA ID' + '/join_tract_msa.shp')

# If you want to include the CMAP region as a whole, you have to do a 
# bit clipping to boundaries. Easily done in QGIS and included as a file.
cmap_msa_tract = gpd.read_file(WD + '/CMAP Tracts 2019/' + '/clipped_cmap_region.shp')

# Cleaning cmap up a bit to ensure we capture the right tracts
# Also noting that these are using 2019 census tracts. It appears there
# are 46 more census tracts in the region since the original creation of EDAs.
cmap_clean = cmap_msa_tract[cmap_msa_tract['NAME_2'] == 
                            'Chicago-Naperville-Elgin, IL-IN-WI'].drop_duplicates('GEOID')

# Cleaning out Chicago from the total tract list. The clipped_cmap_region SF
# has the area we're interested in that
# the MSA captures more of than we want.
#  Note that I don't drop duplicate GEOIDs since some may
# fall within multiple MSAs. We can check and clean at the end to avoid doubles.
all_tract_clean = join_tract_msa[join_tract_msa['NAME_2'] !=
                                 'Chicago-Naperville-Elgin, IL-IN-WI']


# Merging tract sfs with data, one-to-many join has to be done with pandas
# and then reintroduced as a geodataframe.

data_usa = pd.merge(data_19, all_tract_clean)

data_cmap_19 = cmap_clean.merge(data_19)

cmap_eda_19 = eda_flag(data_cmap_19, 'NAME_2', total_race, white, lep_vars,
                    tot_lep_col, med_inc_vars, 'replace')

all_edas = eda_flag(data_usa, 'NAME_2', total_race, white, lep_vars,
                    tot_lep_col, med_inc_vars, 'replace')


# Merging all the tracts back to a geodataframe then sending it to file
# Despite data loss, visual check seems like a thumbs up. Most missing data
# looks like it's where there is no MSA.

all_tracts_edas = all_tract_clean.merge(all_edas)
all_tracts_edas.to_file(WD + '/Recreated USA EDAs' + '/all_msa_edas.shp')

fin_cmap = cmap_clean.merge(cmap_eda_19)
# fin_cmap.to_file(WD + '/Recreated CMAP EDAs/2019 EDAs + '/cmap_eda_recreate_2019.shp')



# Below, I create a new shapefile to validate the function against the original
# CMAP EDA measure.

########## BEGIN VALIDATION ##############

# Loading in a new census variable to work with older data.
c_2010_14 = Census(CENSUS_KEY, year = 2014 )

# Loading in 2014 tracts, clipped in QGIS to the CMAP region
cmap_tract_14 = gpd.read_file(WD + '/CMAP Tracts 2014/clipped_cmap_region_2014.shp')
cmap_tract_14['NAME_2'] = 'CMAP Region'

# Loading in 2014 ACS Data
data_14 = gen_table_county_tract_acs(eda_vars, ['IL'], 'acs5', c_2010_14)

data_cmap_14 = cmap_tract_14.merge(data_14)

# Flagging EDAs, we mark 729 while the original CMAP measure captures 743
# 1.9% undermatch isn't terrible. Visually, things line up, too.
cmap_eda_14 = eda_flag(data_cmap_14, 'NAME_2', total_race, white,
                       lep_vars, tot_lep_col, med_inc_vars, 'replace')

# remerging to geodataframe
eda_14_tracts = cmap_tract_14.merge(cmap_eda_14)

eda_14_tracts.plot(column = 'eda_flag')

# eda_14_tracts.to_file(WD + '/Recreated CMAP EDAs/2014 EDAs' + '/cmap_eda_recreate_2014.shp')



















