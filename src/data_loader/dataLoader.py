import mysql.connector as mc
import os,csv,datetime,argparse,json


parser=argparse.ArgumentParser()
parser.add_argument('--mode',required=False,default='userPassedList')
parser.add_argument('--countryList',required=False)

args=parser.parse_args()
mode=args.mode


def dataBaseConnector(func):
    def createConnection(*args, **kwargs):
        #I have set the credentials as an environment variable here. 
        conn_str = json.loads(os.environ["CONN"])
        dbConnection = mc.connect(**conn_str)
        cursor=dbConnection.cursor()
        try:
            sqlQuery = func(cursor, *args, **kwargs)
        except Exception:
            cursor.rollback()
            print("Database connection error")
            raise
        else:
            dbConnection.commit()
        finally:
            cursor.close()        
        return sqlQuery
    return createConnection

@dataBaseConnector
def createTable(cursor,countryList):
    ## for each country in the the country list- creatte the table
    for country in countryList:  
        try:
            createTableQuery = """CREATE TABLE IF NOT EXISTS Table_{country}   ( 
                             Customer_Name VARCHAR(255) NOT NULL,
                             Customer_Id VARCHAR(18) NOT NULL,
                             Customer_Open_Date Date NOT NULL,
                             Last_Consulted_Date Date, 
                             Vaccination_Type Char(5), 
                             Doctor_Consulted Char(255),
                             State Char(5),
                             Country Char(5), 
                             Date_Of_Birth Date,
                             Active_Customer Char(1),
                             PRIMARY KEY (Customer_Name)) """.format(country=country)
           
            result = cursor.execute(createTableQuery)
            if result:
                print("Country Table created successfully ")
        except mc.Error as error:
            print("Failed to create table in MySQL: {}".format(error))

def getCountryList():
    listOfCountries=[]
    for files in os.listdir('../data_files'):
                if files.endswith('.txt'):
                       listOfCountries.append(os.path.splitext(files)[0])
    return listOfCountries

@dataBaseConnector
def insertIntoTable(cursor,countryList):
    numberOfRowsInserted=0
    try:
    ## for each country in the the country list- open the corresponding file and load to the corresponding table
      for country in countryList:
        with open(dataFilesPath+country+'.txt') as inputFile:
            csvReader=csv.reader(inputFile,delimiter='|')
            next(csvReader)##ignores header line
            for row in csvReader: 
                row[10]=datetime.datetime.strptime(row[10],"%m%d%Y").strftime("%Y%m%d")## cleaning and transforming the Date of Birth column 
                '''
                I have used Replace here so incase of duplicates it will ovewrite.
                 And in case of a new value it will insert. 
                 We can use Insert here as well if want to identify and handle duplicates separately.
                '''
                query='REPLACE INTO Table_{country} (Customer_Name, Customer_Id, Customer_Open_Date, Last_Consulted_Date, Vaccination_Type, Doctor_Consulted, State, Country, Date_Of_Birth, Active_Customer) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(country=country) 
                cursor.execute(query,row[2:])
                numberOfRowsInserted+=cursor.rowcount
    except mc.Error as error:
            print("Failed to insert into table: {}".format(error))
    except OSError as e:
            print("File Not Found: {}".format(e))

    if numberOfRowsInserted!=0:
        print("{} record(s) inserted successfully".format(numberOfRowsInserted))

if __name__=='__main__':
    dataFilesPath='../data_files/'
    if mode=='batch':
        countryList=getCountryList()
    else:
        countryList=args.countryList.split(",")
        print(countryList)
    createTable(countryList)
    insertIntoTable(countryList)


