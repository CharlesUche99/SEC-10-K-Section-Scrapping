import re
import os
from datetime import datetime
import pandas as pd



#Specify path of folder containing all 10-K filings
#The cleaned version of 10-K files can be accessed from https://sraf.nd.edu/sec-edgar-data/cleaned-10x-files/
source_path = 'D:\\NLP DATA CLEANING\\All Files'


#Create a folder to save the scrapped output
new_folder = "MDA" #Change this if scrapping a different section
new_folder_path = os.path.join(source_path, new_folder)
isExist = os.path.exists(new_folder_path)
if not isExist:
        # Create the folder if it does not exist
        os.makedirs(new_folder_path)
        print("The new directory is created!")


# list to store file names
file_names_list = []
# Iterate directory to select only text files (incase there are other file types in the directory)
for file in os.listdir(source_path):
    # check only text files
    if file.endswith('.txt'):
        file_names_list.append(file)


#This loop returns the 10ks by splitting the string name on "_" and selecting the 2nd element in the resulting list.
annual_reports = []
for name in file_names_list:
    name_breakdown = re.split('_', name)
    if name_breakdown[1] == "10-K": #This line ensures that only 10-K files are selected (It excludes 10-Qs)
        annual_reports.append(name)
print("There are " + str(len(annual_reports)) + " annual reports in this folder")

#Use an error catcher for any unforseen issues.
index = 0
for file in annual_reports:
    index += 1
    print("Now scrapping for file number: " + str(index))
    try:
        file_path = os.path.join(source_path, file)
        text_file = open(file_path)
        file_content = text_file.read()

                # Grab and store the 10K text body
        TenKtext = file_content
        TenKtext = TenKtext.replace('\n', ' ') # Replace \n (new line) with space

        #Find file reporting date (this is different from the filing date)
        matches_date = re.compile(r'(conformed\speriod\sof\sreport)', re.IGNORECASE)
        matches_array_date = pd.DataFrame([(match.group(), match.start()) for match in matches_date.finditer(TenKtext)])
        matches_array_date.columns = ['SearchTerm', 'Start']
        date_start = matches_array_date['Start'].iloc[0]
        TenKItem8_date = TenKtext[date_start:(date_start + 37)]
        TenKItem8_date = TenKItem8_date.translate(' \n\t\r\s')
        date = TenKItem8_date[-9:]

        # Set up the regex pattern
        #This particular version of the code scraps for the MDA. Please see commented out sections below for NTFS and Risk Section.
        #The code can be adapted for any other section.
        matches = re.compile(r'(item\s(7[\.\s]|7[\s]|7[\:\s]|8[\.\s]|8[\s]|8[\:\s])|'
                             'discussion\sand\sanalysis\sof\s(consolidated\sfinancial|financial)\scondition|'
                             '(audited\sfinancial|consolidated\sfinancial|financial)\sstatements\sand\ssupplementary|'
                             '(audited\sfinancial|consolidated\sfinancial|financial)\sstatement\sand\ssupplementary)', re.IGNORECASE)

        ''' Uncomment this variable and comment the one above to scrap for Item 8 (Financial Statement)
        matches = re.compile(r'(item\s(8[\.\s]|8[\s]|8[\:\s]|9[\.\s]|9[\s]|9[\:\s])|'
                                '(audited\sfinancial|consolidated\sfinancial|financial)\sstatements\sand\ssupplementary\sdata|'
                                'changes\sin\sand\sdisagreement|'
                                'changes\sand\sdisagreement)', re.IGNORECASE)
        '''

        matches_array = pd.DataFrame([(match.group(), match.start()) for match in matches.finditer(TenKtext)])

        # Set columns in the dataframe
        matches_array.columns = ['SearchTerm', 'Start']

        # Get the number of rows in the dataframe
        Rows = matches_array['SearchTerm'].count()


            
        # Create a new column in 'matches_array' called 'Selection' and add adjacent 'SearchTerm' (i and i+1 rows) text concatenated
        count = 0 # Counter to help with row location and iteration
        while count < (Rows-1): # Can only iterate to the second last row
            matches_array.at[count,'Selection'] = (matches_array.iloc[count,0] + matches_array.iloc[count+1,0]).lower() # Convert to lower case
            count += 1


        # Set up 'Item 7/8 Search Pattern' regex patterns
        matches_item7 = re.compile(r'(item\s7\.discussion\s[a-z]*)')
        matches_item8 = re.compile(r'(item\s8\.(audited\sfinancial|consolidated\sfinancial|financial)\s[a-z]*)')
        #Uncomment the variable below if you want to scrap for item 8 (NTFS)
        ''' matches_item9 = re.compile(r'(item\s(9[\.\s]|9[\s])'
                                'changes\s[a-z]*)') '''
        
        # Find and store the locations of Item 8/9 Search Pattern matches
        matches_array = matches_array.dropna(how='any')

        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # If you want to scrap item 7 (MDA), please leave matches_item7 and matches_item8 below.
        #However, if you want to scrap for item 8 (NTFS), please change matches_item7 to matches_item8 and matches_item8 to matches_item9 below.
        newdf = matches_array.loc[matches_array.stack().str.findall(matches_item7).unstack().any(1)]
        Start_Loc = newdf.iloc[-1,1]

        newdf2 =  matches_array.loc[matches_array.stack().str.findall(matches_item8).unstack().any(1)]
        End_Loc = newdf2.iloc[-1,1]

        #condition to reset the start location if it occurs subsequent to the end location    
        if   Start_Loc >  End_Loc:
            Start_Loc = newdf.iloc[-2,1]
            if   Start_Loc >  End_Loc:
                Start_Loc = newdf.iloc[-3,1]
                if   Start_Loc >  End_Loc:
                    Start_Loc = newdf.iloc[-4,1]
                    if   Start_Loc >  End_Loc:
                        Start_Loc = newdf.iloc[-5,1]
                        if   Start_Loc >  End_Loc:
                            Start_Loc = newdf.iloc[-6,1]
                            if   Start_Loc >  End_Loc:
                                Start_Loc = newdf.iloc[-7,1]
        else:
            Start_Loc = newdf.iloc[-1,1]


        # Extract section of text and store in 'TenKItem'
        TenKItem = TenKtext[Start_Loc:End_Loc]

        # Clean newly extracted text
        TenKItem = TenKItem.strip() # Remove starting/ending white spaces
        TenKItem = TenKItem.replace('\n', ' ') # Replace \n (new line) with space
        TenKItem = TenKItem.replace('\r', '') # Replace \r (carriage returns-if you're on windows) with space
        TenKItem = TenKItem.replace('&nbsp;', ' ') # Replace "&nbsp;" (a special character for space in HTML) with space
        TenKItem = TenKItem.replace('&#160;', ' ') # Replace "&#160;" (a special character for space in HTML) with space
        while '  ' in TenKItem:
            TenKItem = TenKItem.replace('  ', ' ') # Remove extra spaces

        # path to save the scrapped document
        new_filename = str(date) + "_" + str(file)
        scrapped_path = os.path.join(new_folder_path, new_filename)
        


        with open(scrapped_path, 'w', encoding='utf-8') as f:
            f.write(TenKItem)



    #except Exception as e: print(e)
    except:
        #create a folder with a textfile for files that could not be scrapped.
        failed_folder = "Failed Scrapping MDA"
        failed_folder_path = os.path.join(source_path, failed_folder)
        isExist = os.path.exists(failed_folder_path)
        if not isExist:
        # Create the folder if it does not exist
            os.makedirs(failed_folder_path)
            print("The new directory is created!")
        

        #The failed text document
        failed_text = "error files.txt"
        failed_textfile_path = os.path.join(failed_folder_path, failed_text)
        with open(failed_textfile_path, 'a') as f:
            f.write(file)
            f.write("\n")
print("DONE")
