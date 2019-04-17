/*
   prRestoreDatabase - Versão 1.0 - 06/07/2016
                       Versão 2.0 - 16/09/2016

   Criado por Logan Destefani Merazzi (logan@merazzi.com.br) - http://blog.merazzi.com.br

   Procedure para facilitar nos processos de restauração de um banco de dados
   Necessário um backup full e os "N" backups de log de transação subsequente.

Para executar:
DECLARE @origin_database_name varchar(50) = 'OriginDB'
DECLARE @new_database_name varchar(50) = 'DestinationDB'
DECLARE @last_diff bit = 1
DECLARE @log_qty smallint = 3
DECLARE @data_destination_path varchar(100) = 'G:\Data\Destination\path'
DECLARE @log_destination_path varchar(100) = 'L:\Logs\Destinarion\Path'

EXEC master..prRestoreDatabase @origin_database_name, @new_database_name, @last_diff, @log_qty, @data_destination_path, @log_destination_path

Últimas alterações:
V 2.0: Novo parâmetro: @last_diff. Agora, é possível restaurar o diferencial e especificar quantos logs após ele será restaurado.
V 1.0: Criação do script

To-Do List:
- Melhorar a apresentação e validação dos backups para que em caso de erro nas etapas anteriores, as demais não continuem.
- Validar utilização da base, matando os processos

Bugs conhecidos: Até o momento, nenhum (a.k.a: na minha máquina funciona).
Bugs desconhecidos: Nenhum (até pq se eu soubesse não seria desconhecido... dã!)

Script disponível para download gratuito (se você pagou por ele, me diz, porque você faria isso?) e serve para uso comercial como não comercial.
Apenas peço para que os créditos sejam mantidos quando for utilizá-lo.

*/

USE master

GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'prRestoreDatabase')
  EXEC ('CREATE PROCEDURE dbo.prRestoreDatabase AS SELECT ''Objeto vazio. Deve ser feito o ALTER do objeto''')
GO

ALTER PROCEDURE dbo.prRestoreDatabase (@origin_database_name varchar(50), @new_database_name varchar(50), @last_diff bit = 0, @log_qty smallint = 0, @data_destination_path varchar(100), @log_destination_path varchar(100))
AS
DECLARE @backup_cmd nvarchar(2000)

;with cte_full as
(
  select top 1 bs.backup_set_id, bs.media_set_id, bs.backup_start_date, bs.database_name, bmf.physical_device_name
  from msdb.dbo.backupset bs
  join msdb.dbo.backupmediafamily bmf
  on bs.media_set_id = bmf.media_set_id
  where database_name = @origin_database_name
  and type = 'D' -- FULL
  order by backup_start_date desc
), CTE_MOVE (MOVE_PATH) AS 
(
select 
  ', MOVE N''' + logical_name + ''' TO N''' +
  case file_type when 'D' 
    then @data_destination_path + reverse(substring(reverse(physical_name),1,charindex('\',reverse(physical_name))-1))
	else @log_destination_path + reverse(substring(reverse(physical_name),1,charindex('\',reverse(physical_name))-1))
  end + ''''
from cte_full cte
join msdb.dbo.backupfile bf
on cte.backup_set_id = bf.backup_set_id
for xml path('')
)
SELECT @backup_cmd  = 
       N'RESTORE DATABASE ' + @new_database_name + 
       ' FROM DISK = N''' + physical_device_name + ''' '+ 'WITH FILE = 1, ' +
       STUFF(MOVE_PATH,1,1,'') + ', REPLACE, NORECOVERY,  NOUNLOAD,  STATS = 5'
FROM CTE_MOVE, cte_full

exec sp_executesql @backup_cmd

if @last_diff = 1
begin
  set @backup_cmd = N''
  -- Verificar o último diferencial gerado - se houver
  ;with cte_dif as (
  select top 1 bs.backup_set_id, bs.media_set_id, bs.backup_start_date, bs.database_name, bmf.physical_device_name
  from msdb.dbo.backupset bs
  join msdb.dbo.backupmediafamily bmf
  on bs.media_set_id = bmf.media_set_id
  where database_name = @origin_database_name
  AND type = 'I'
  order by backup_start_date desc
  ), CTE_FILE (DATA_FILE) AS (
select 
  ', FILE = N''' + logical_name + ''''
  from cte_dif cte
  join msdb.dbo.backupfile bf
  on cte.backup_set_id = bf.backup_set_id
  where file_type = 'D' -- Exclui os arquivos de log
  for xml path('')
)
  select @backup_cmd  = 
  'RESTORE DATABASE ' + @new_database_name +
  STUFF(DATA_FILE,1,1,'') + 
  ' FROM DISK = N''' + physical_device_name + ''' '+ 'WITH FILE = 1, ' +
  '  NORECOVERY,  NOUNLOAD,  STATS = 10'
  FROM CTE_FILE, cte_dif
  
  exec sp_executesql @backup_cmd

end

if @log_qty > 0
begin
	set @backup_cmd = N''
	;with cte_media as
	(
	  select top 1 bs.backup_set_id, bs.media_set_id, bs.backup_start_date, bs.database_name, bs.first_lsn, bs.position
	  from msdb.dbo.backupset bs
	  where database_name = @origin_database_name
	  and  -- Definir se os logs serão restaurados baseado no full ou no diferencial.
    (
      (@last_diff = 0 and type = 'D') -- FULL
      OR
      (@last_diff = 1 and type = 'I') -- DIFF
    )
	  order by backup_start_date desc
	)
	select top (@log_qty) @backup_cmd = 
    @backup_cmd + N'RESTORE LOG ' + @new_database_name + ' FROM DISK = N''' + bmf.physical_device_name + ''' WITH  FILE = ' + cast(bs.position as varchar(5)) +',  NORECOVERY,  NOUNLOAD,  STATS = 10; '
	  from cte_media cte
	  join msdb.dbo.backupset bs
    on (
         (@last_diff = 0) and (cte.first_lsn = bs.database_backup_lsn)
         OR 
         (@last_diff = 1) and (cte.first_lsn = bs.checkpoint_lsn)
       )
	  join msdb.dbo.backupmediafamily bmf
	  on bs.media_set_id = bmf.media_set_id
	  where bs.database_name = @origin_database_name
	  and type = 'L' -- Logs
	  order by bs.backup_start_date

	exec sp_executesql @backup_cmd

end

set @backup_cmd = N''
select @backup_cmd = N'RESTORE DATABASE ' + @new_database_name + ' WITH RECOVERY'
exec sp_executesql @backup_cmd