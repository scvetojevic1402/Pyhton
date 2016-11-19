#FOR NEEDS OF ONE OF THE STDIES I HAD LARGE NUMBER OF PHOTOS IN A SINGLE FOLDER
#PHOTOS NEEDED TO BE SPLIT INTO E.G. 32 DIFFERENT FOLDERS CONTAINING EQUAL COUNT OF PHOTOS
from os import listdir
import os
#import shutil

path = 'C:\\Users\\User\\Documents\\Tweets_analysis\\Pictures\\Profanity_filtering\\'
new_path = 'C:\\Users\\User\\Documents\\Tweets_analysis\\Pictures\\Profanity_filtering_split_to_folders\\'
files = listdir(path)
print(len(files))

x=0
y=0
for i in range(0,len(files)-1):
    newest_path=new_path+'{0}\\'.format(x)
    if not os.path.exists(newest_path):
            os.makedirs(newest_path, 777)
    os.rename('{0}{1}'.format(path,files[i]),'{0}{1}'.format(newest_path,files[i]))
    y=y+1
    #print(path+files[i])
    #print('{0}{1}'.format(newest_path,files[i]))
    if(divmod(y,1000)[1]==0):
        x=x+1
        print(x)
