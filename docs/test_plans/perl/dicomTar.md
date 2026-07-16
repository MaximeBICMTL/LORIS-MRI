# Test plan for `dicomTar.pl`

## Manual tests

- Run `dicomTar.pl` without the `-database` flag and ensure nothing gets inserted into the DB.
- Run `dicomTar.pl` with the `-database` flag and check that entries got inserted into the DB in the `tarchive*` tables correctly.
- Run `dicomTar.pl` with the `-mri_upload_update` flag on a DICOM directory that was not previously uploaded via the imaging uploader (ensure there is nothing in `mri_upload` table for that study before running the script) => ensure that an entry has been added to `mri_upload` for the uploaded DICOM study.
- Run `updateMri_upload.pl` on a DICOM archive and ensure an MRI upload is created.
