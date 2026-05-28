CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_SYNC_PARAM_DEV
AS

BEGIN

EXECUTE IMMEDIATE 'truncate table SegmentationMapping';
	
insert into SegmentationMapping 
(
	 "pkid"
	,"syscode_segmentation_lv1"
--	,"version"
	,"segment_name_lv1"
	,"syscode_segmentation_lv2"
	,"segment_name_lv2"
	,"syscode_segmentation_lv3"
	,"segment_name_lv3"
	,"syscode_segmentation_lv4"
	,"segment_name_lv4"
	,"syscode_segmentation_lv5"
	,"segment_name_lv5"
	,"syscode_segmentation_lv6"
	,"segment_name_lv6"
	,"syscode_segmentation_lv7"
	,"segment_name_lv7"
	,"syscode_segmentation_lv8"
	,"segment_name_lv8"
	,"syscode_segmentation_lv9"
	,"segment_name_lv9"
	,"syscode_segmentation_lv10"
	,"segment_name_lv10"
	,"merge_sql_conditions"
	,"created_by"
	,"created_date"
	,"created_host"
	,"updated_by"
	,"updated_date"
	,"updated_host"
	,"is_deleted"
	,"deleted_by"
	,"deleted_date"
	,"deleted_host"

)	
SELECT
	 "pkid"
	,"syscode_segmentation_lv1"
--	,"version"
	,"segment_name_lv1"
	,"syscode_segmentation_lv2"
	,"segment_name_lv2"
	,"syscode_segmentation_lv3"
	,"segment_name_lv3"
	,"syscode_segmentation_lv4"
	,"segment_name_lv4"
	,"syscode_segmentation_lv5"
	,"segment_name_lv5"
	,"syscode_segmentation_lv6"
	,"segment_name_lv6"
	,"syscode_segmentation_lv7"
	,"segment_name_lv7"
	,"syscode_segmentation_lv8"
	,"segment_name_lv8"
	,"syscode_segmentation_lv9"
	,"segment_name_lv9"
	,"syscode_segmentation_lv10"
	,"segment_name_lv10"
	,"merge_sql_conditions"
	,"created_by"
	,"created_date"
	,"created_host"
	,"updated_by"
	,"updated_date"
	,"updated_host"
	,"is_deleted"
	,"deleted_by"
	,"deleted_date"
	,"deleted_host"
from "SegmentationMapping"@DBCONFIGLINK;


END;