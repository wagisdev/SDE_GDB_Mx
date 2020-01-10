#-------------------------------------------------------------------------------
# Name:        Database Maintenance
# Purpose:  This script is designed to pull and push feature classes and tables
#           between databases in the Testing, Staging and Production environment.
#           This script was designed to follow the 2018 GIU business model/flow.
#           At some point in the future the script will allow for different flows.
#
# Author:      John Spence, Spatial Data Administrator, City of Bellevue
#
# Created:     24 December 2018
# Modified:    11 November 2019
# Modification Purpose:  Added in e-mail and additional functions.
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# The initial configuration for the script comes from a single AD SDE connection
# that is set below for the variable db_connection.
#
# ------------------------------- Dependencies ---------------------------------
# 1) AdminGTS.View_Find_Layers = Sorts FC vs. Tables
# 2) AdminGTS.View_Layer_Table_History = Id's Schema, Table Names, Type, and Dates
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Configure only hard coded db connection here.
db_connection = r'Database Connections\\YourDatabase.sde'

# Targeted Dbase Server.
db_table = '[ADMINGTS].[View_Layer_Table_History]'

# Configure database update type here. (Prod, Stg, Test, Other)
db_type = 'STG'

# Send confirmation of rebuild to
email_target = 'your@email.com'

# Configure the e-mail server and other info here.
mail_server = 'smtprelay.yours.com'
mail_from = 'DB Maintenance<noreply@yours.com>'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import arcpy, smtplib, os, datetime

#-------------------------------------------------------------------------------
#
#
#                                 MAINTENENCE MODULES
#
#
#-------------------------------------------------------------------------------

# Def for determining what database our connection is tied to.
def set_current_database(db_connection):
    ##print ("Entering Set Current Database----")
    global current_db
    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_db_sql)
    current_db = check_db_return
    print ("Current Database:  " + current_db)
    ##print ("----Leaving Set Current Database")
    return current_db


def check_FC_count (db_connection, db_table):
    ##print ('Entering Database Check Feature Class Count----')

    # Define global variable to carry through application.
    global mx_count

    # Poll table for number of feature classes from admingts.view_layer_table_history
    db_table_sql = db_table
    fc_type_sql = "'" + 'Feature Class' + "'"
    check_FC_count_sql = ('''SELECT count (type) as Feature_Classes FROM {0} where [Type] = {1}'''
    .format(db_table_sql,fc_type_sql))
    check_FC_count_return = int(arcpy.ArcSDESQLExecute(db_connection).execute(check_FC_count_sql))
    mx_count = check_FC_count_return

    print ('Processing {0} feature classes.'.format(mx_count))
    ##print ('----Exiting Database Check Feature Class Count')


    return mx_count


#  Obtain database connection used for maintenance.
def obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner):

    ##print ("Entering Obtain Database Connection----")
    # Define Global Variable
    global conn_string

    # Prep variables for SQL Query
    target_db_sql = "'" + target_db + "'"
    db_type_sql = "'" + target_db_type + "'"
    fc_update_owner_sql = "'" + fc_update_owner + "'"

    # Pull from Dbase exact connection string needed.
    try:
        db_connection_stringSQL = '''select * from admingts.SDE_Connections where SourceDB = {0} and SourceDB_Type = {1} and Data_Owner = {2}'''.format(target_db_sql, db_type_sql, fc_update_owner_sql)
        db_connection_stringReturn = arcpy.ArcSDESQLExecute(db_connection).execute(db_connection_stringSQL)
        for row in db_connection_stringReturn:
            conn_string = row [4]

            print ("     Database connection = {0} \n".format(conn_string))
    except:
            print ("     No Connection for:  {0}.".format(fc_update_owner))


    ##print ("----Leaving Obtain Database Connection")
    return conn_string


# Used to recalculate the extents.
def recalc_extents(conn_string, fc_target_fullnamewdb):

    ##print ("Entering Recalcaulte Extents----")
    print ("     Calculating Extent:  {0}".format(fc_target_fullnamewdb))

    output_connection = conn_string + '\\' + fc_target_fullnamewdb
    try:
        arcpy.RecalculateFeatureClassExtent_management(output_connection)
        print ("     --Status:  Success!\n")
    except Exception as error_extents:
        print ("     --Status:  Failure!\n")
        print(error_extents.args[0])

    ##print ("----Leaving Recalcaulte Extents")
    return

# Used to recalculate the indexes.  Not really required for MS SQL as the spatial extent is auto calculated.
def recalc_indexes(conn_string, fc_target_fullnamewdb):

    ##print ("Entering Recalcaulte Indexes----")
    print ("     Indexing:  {0}".format(fc_target_fullnamewdb))

    try:
        arcpy.RebuildIndexes_management(conn_string, "NO_SYSTEM",fc_target_fullnamewdb, "ALL")
        print ("     --Status:  Success!\n")
    except Exception as error_indexes:
        print ("     --Status:  Failure!\n")
        print(error_indexes.args[0])

    ##print ("----Leaving Recalcaulte Indexes")

    return

# Used to update statistics on the FC.
def analyze_fc(conn_string, fc_target_fullnamewdb):

    ##print ("Entering Analyze Feature Class----")
    print ("     Analyze:  {0}".format(fc_target_fullnamewdb))

    try:
        arcpy.AnalyzeDatasets_management(conn_string,
                                     "NO_SYSTEM",
                                     fc_target_fullnamewdb,
                                     "ANALYZE_BASE",
                                     "ANALYZE_DELTA",
                                     "ANALYZE_ARCHIVE")
        print ("     --Status:  Success!\n")
    except Exception as error_analyze:
        print ("     --Status:  Failure!\n")
        print(error_analyze.args[0])

    ##print ("----Leaving Analyze Feature Class")

    return

# Main MX routine def that uses extents, indexes and analyze to update each FC one at a time.
def perform_mx_routine(conn_string, fc_target_fullnamewdb):

    print ("Started:  {0}\n".format (datetime.datetime.now()))
    recalc_extents(conn_string, fc_target_fullnamewdb)
    analyze_fc(conn_string, fc_target_fullnamewdb)
    recalc_indexes(conn_string, fc_target_fullnamewdb)
    analyze_fc(conn_string, fc_target_fullnamewdb)
    print ("Completed:  {0}".format (datetime.datetime.now()))

    return

def sendcompletetion(email_target, mail_server, mail_from, current_db, message_body, startDT, finishDT):
    mail_priority = '5'
    mail_subject = '{0}:  Maintenance Process Completed'.format(current_db)
    mail_msg = 'Maintenance process has completed for {0}.  {1}\nTask Start: {2}\nTask End: {3}\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(current_db, message_body, startDT, finishDT)

    # Set SMTP Server and configuration of message.
    server = smtplib.SMTP(mail_server)
    email_target = email_target
    mail_priority = mail_priority
    mail_subject =  mail_subject
    mail_msg =  mail_msg

    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_msg)
    # Double commented out code hides how to send a BCC as well.
    ##send_mail = 'To: {0}\nFrom: {1}\nBCC: {2}\nX-Priority: {3}\nSubject: {4}\n\n{5}'.format(email_target, mail_from, mail_bcc, mail_priority, mail_subject, mail_msg)

    server.sendmail(mail_from, email_target, send_mail)
    # Double commented out code hides how to send a BCC as well.
    ##server.sendmail(mail_from, [email_target, mail_bcc], send_mail)

    server.quit()

    return

#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

# Get starting date/time
startDT = datetime.datetime.now()

# Determine which database we are working in.
set_current_database(db_connection)

# Determine how many FC's need to be updated.
check_FC_count(db_connection,db_table)

# Begin Rolling through layers for MX.  At end of count, script terminates.
if mx_count > 0:

# Set Zero count
    completed_mx = 0

# Run query selecting FC Owner and FC for tageted MX needs.
    db_table_sql = db_table
    fc_type_sql = "'" + 'Feature Class' + "'"
    fc_targeting_sql = ('''SELECT [Schema], [Table_Name] FROM {0} where [Type] = {1}''').format(db_table_sql,fc_type_sql)
    fc_targeting_return = arcpy.ArcSDESQLExecute(db_connection).execute(fc_targeting_sql)
    for row in fc_targeting_return:
        fc_target_schema = row[0]
        fc_taret_tablename = row[1]

        print '\n\n-----------------------------------------------------'
        print 'Current Database:  {0}'.format(current_db)
        print 'FC Targeted Schema:  {0}'.format(fc_target_schema)
        print 'FC Targeted for MX:  {0}.{1}'.format(fc_target_schema, fc_taret_tablename)

        print '\nBeginning MX Cycle...stand back as it shakes, rattles and rolls.\n'

        # Massage SQL Output for MX Routine
        fc_target_fullname = fc_target_schema + '.' + fc_taret_tablename
        fc_target_fullnamewdb = current_db + '.' + fc_target_fullname

        # Obtain database connection
        # Set vars
        target_db = current_db
        target_db_type = db_type
        fc_update_owner = fc_target_schema

        # Send vars to def to get connection
        obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner)

        # Send off to MX
        perform_mx_routine(conn_string, fc_target_fullnamewdb)
        completed_mx += 1

        print ("\n\nCompleted {0} of {1} available!\n\n".format (completed_mx, mx_count))

    # Get finish date/time
    finishDT = datetime.datetime.now()
    print ("Maintenance Complete.  Please review error log.")
    message_body = 'Please review error log when capable.'
    sendcompletetion(email_target, mail_server, mail_from, current_db, message_body, startDT, finishDT)

else:
    print "No SDE data in database.  No MX can be performed at this time."




