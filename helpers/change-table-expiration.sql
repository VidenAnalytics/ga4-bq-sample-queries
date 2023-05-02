DECLARE list_of_tables ARRAY<STRING>;
DECLARE counter INT64 DEFAULT 0;

EXECUTE IMMEDIATE
  'SELECT ARRAY(SELECT distinct table_name FROM `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`)'
  INTO list_of_tables;

LOOP
  IF counter = ARRAY_LENGTH(list_of_tables) THEN
    LEAVE;
  END IF;

  EXECUTE IMMEDIATE
    CONCAT('ALTER TABLE `your_project.your_dataset.',list_of_tables[offset(counter)],'` SET OPTIONS(expiration_timestamp=null)');

  SET counter = counter + 1;
END LOOP;
