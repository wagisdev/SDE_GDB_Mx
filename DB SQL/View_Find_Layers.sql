USE [Carta]
GO

/****** Object:  View [ADMINGTS].[View_Find_Layers]    Script Date: 1/10/2020 9:58:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [ADMINGTS].[View_Find_Layers]
AS

SELECT t.[TABLE_SCHEMA] as 'Schema_Name', t.[TABLE_NAME] as 'Table_Name', FC_Type = 'Feature Class'
  FROM [Carta].[INFORMATION_SCHEMA].[TABLES] t
  inner join [INFORMATION_SCHEMA].[COLUMNS] c
  on t.TABLE_NAME=c.TABLE_NAME
  where c.COLUMN_NAME='shape' and t.TABLE_SCHEMA not in ('dbo','sde') and t.TABLE_TYPE='BASE TABLE'

Union

SELECT o.[TABLE_SCHEMA] as 'Schema_Name', o.[TABLE_NAME] as 'Table_Name',FC_Type = 'Table'
	FROM [Carta].[INFORMATION_SCHEMA].[TABLES] o
	where o.TABLE_NAME not in (SELECT distinct [TABLE_NAME] FROM [Carta].[INFORMATION_SCHEMA].[COLUMNS] m where m.COLUMN_NAME='shape')
		and TABLE_SCHEMA not in ('SDE') 
		and TABLE_NAME not like 'i[0-9]%' 
		and TABLE_NAME not like 'sde%'
		and TABLE_NAME not like '%_LOX'
		and TABLE_NAME not like '%guid%'
		and TABLE_TYPE='Base Table'

GO


