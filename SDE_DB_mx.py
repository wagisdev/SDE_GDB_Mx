#-------------------------------------------------------------------------------
# Name:        Database Maintenance
# Purpose:  This script is designed to pull and push feature classes and tables
#           between databases in the Testing, Staging and Production environment.
#           This script was designed to follow the 2018 GIU business model/flow.
#           At some point in the future the script will allow for different flows.
#
# Author:      John Spence, Spatial Data Administrator, City of Bellevue
#
# Created:
# Modified:
# Modification Purpose:
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
db_connection = r'Database Connections\\Connection to Carta on COBSSDB16TS18B.sde'

# Targeted Dbase Server.
db_table = '[ADMINGTS].[View_Master_Layer_Table_History]'

# Configure database update type here. (Prod, Stg, Test, Other)
db_type = 'Test'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import arcpy

#-------------------------------------------------------------------------------
#
#
#                                 MAINTENENCE MODULES
#
#
#-------------------------------------------------------------------------------

# Def for determining what database our connection is tied to.
def set_current_database(db_connection):
    print "Entering Set Current Database----"
    global current_db
    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_db_sql)
    current_db = check_db_return
    print "Current Database:  " + current_db
    print "----Leaving Set Current Database"
    return current_db


def check_FC_count (db_connection, db_table):
    print 'Entering Database Check Feature Class Count----'

    # Define global variable to carry through application.
    global mx_count

    # Poll table for number of feature classes from admingts.view_layer_table_history
    db_table_sql = db_table
    fc_type_sql = "'" + 'Feature Class' + "'"
    check_FC_count_sql = ('''SELECT count (type) as Feature_Classes FROM {0} where [Type] = {1}'''
    .format(db_table_sql,fc_type_sql))
    check_FC_count_return = int(arcpy.ArcSDESQLExecute(db_connection).execute(check_FC_count_sql))
    mx_count = check_FC_count_return

    print'Processing {0} feature classes.'.format(mx_count)
    print '----Exiting Database Check Feature Class Count'


    return mx_count


#  Obtain database connection used for maintenance.
def obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner):

    print "Entering Obtain Database Connection----"
    # Define Global Variable
    global conn_string

    # Prep variables for SQL Query
    target_db_sql = "'" + target_db + "'"
    db_type_sql = "'" + target_db_type + "'"
    fc_update_owner_sql = "'" + fc_update_owner + "'"

    # Pull from Dbase exact connection string needed.
    db_connection_stringSQL = '''select * from admingts.SDE_Connections where SourceDB = {0} and SourceDB_Type = {1} and Data_Owner = {2}'''.format(target_db_sql, db_type_sql, fc_update_owner_sql)
    db_connection_stringReturn = arcpy.ArcSDESQLExecute(db_connection).execute(db_connection_stringSQL)
    for row in db_connection_stringReturn:
        conn_string = row [4]

        print "Database connection = {0} \n".format(conn_string)
        print "----Leaving Obtain Database Connection"
        return conn_string


# Used to recalculate the extents.
def recalc_extents(conn_string, fc_target_fullnamewdb):

    print "Entering Recalcaulte Extents----"
    print "Targeting:  {0}".format(fc_target_fullnamewdb)

    output_connection = conn_string + '\\' + fc_target_fullnamewdb
    try:
        arcpy.RecalculateFeatureClassExtent_management(output_connection)
        print "Status:  Success!"
    except Exception as error:
        print "Status:  Failure!"
        print(error.args[0])

    print "----Leaving Recalcaulte Extents"
    return

# Used to recalculate the indexes.  Not really required for MS SQL as the spatial extent is auto calculated.
def recalc_indexes(conn_string, fc_target_fullnamewdb):

    print "Entering Recalcaulte Indexes----"
    print "Targeting:  {0}".format(fc_target_fullnamewdb)

    try:
        arcpy.RebuildIndexes_management(conn_string, "NO_SYSTEM",fc_target_fullnamewdb, "ALL")
        print "Status:  Success!"
    except Exception as err:
        print "Status:  Failure!"
        print(error.args[0])

    print "----Leaving Recalcaulte Indexes"

    return

# Used to update statistics on the FC.
def analyze_fc(conn_string, fc_target_fullnamewdb):

    print "Entering Analyze Feature Class----"
    print "Targeting:  {0}".format(fc_target_fullnamewdb)

    try:
        arcpy.AnalyzeDatasets_management(conn_string,
                                     "NO_SYSTEM",
                                     fc_target_fullnamewdb,
                                     "ANALYZE_BASE",
                                     "ANALYZE_DELTA",
                                     "ANALYZE_ARCHIVE")
        print "Status:  Success!"
    except Exception as error:
        print "Status:  Failure!"
        print(error.args[0])

    print "----Leaving Analyze Feature Class"

    return

# Main MX routine def that uses extents, indexes and analyze to update each FC one at a time.
def perform_mx_routine(conn_string, fc_target_fullnamewdb):

    recalc_extents(conn_string, fc_target_fullnamewdb)
    recalc_indexes(conn_string, fc_target_fullnamewdb)
    analyze_fc(conn_string, fc_target_fullnamewdb)

    return



#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

# Determine which database we are working in.
set_current_database(db_connection)

# Determine how many FC's need to be updated.
check_FC_count(db_connection,db_table)

# Begin Rolling through layers for MX.  At end of count, script terminates.
if mx_count > 0:

# Run query selecting FC Owner and FC for tageted MX needs.
    db_table_sql = db_table
    fc_type_sql = "'" + 'Feature Class' + "'"
    fc_targeting_sql = ('''SELECT [Schema], [Table_Name] FROM {0} where [Type] = {1}''').format(db_table_sql,fc_type_sql)
    fc_targeting_return = arcpy.ArcSDESQLExecute(db_connection).execute(fc_targeting_sql)
    for row in fc_targeting_return:
        fc_target_schema = row[0]
        fc_taret_tablename = row[1]

        print '/n/n-----------------------------------------------------'
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

    print "Maintenance Complete.  Please review error log."

else:
    print "No SDE data in database.  No MX can be performed at this time."



