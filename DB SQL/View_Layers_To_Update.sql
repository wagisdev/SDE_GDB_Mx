USE [Carta]
GO

/****** Object:  View [ADMINGTS].[View_Layers_To_Update]    Script Date: 1/10/2020 9:58:25 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [ADMINGTS].[View_Layers_To_Update]
AS

select Schema_Name([schema_id]) as 'Data_Owner', replace(z.name,(DB_NAME() + '.' + Schema_Name([schema_id]) + '.'),'' ) as 'Table_Name', 
z.name as 'SDE_Name', y.FC_Type as 'Type', x.create_date as 'Date_Created', x.modify_date as 'Date_Last_Modified'
--, c.Last_User_Update as 'Last_User_Update'
from sys.tables as x
inner join [ADMINGTS].[View_Find_Layers] as y
on x.name = y.[Table_Name] and Schema_Name([schema_id]) = y.Schema_Name
inner join [sde].[GDB_ITEMS] as z
on (Schema_Name + '.' + y.[Table_Name]) = right(z.name, len(z.name) - charindex('.', z.name))
inner join [ADMINGTS].[View_Layer_Table_History] as c on x.name = c.Table_Name
where cast(x.modify_date as date) = CAST(CURRENT_TIMESTAMP AS DATE)

GO


