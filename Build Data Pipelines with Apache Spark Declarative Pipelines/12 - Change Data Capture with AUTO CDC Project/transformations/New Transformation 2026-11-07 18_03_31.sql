SELECT
  _metadata.file_name as source_file,
  *
FROM read_files(
  '/Volumes/' || 'learn_build_data_pipelines_sdp_joemar_brillo' || '/sdp_lab_1_bronze/lab_files',
  format => 'CSV'
)
ORDER BY source_file, EmployeeID, ProcessDate DESC;