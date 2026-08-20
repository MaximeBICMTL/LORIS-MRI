# Test plan for `convert-dicom-archive-to-bids`

## Manual tests

- If not already done, source the environment file: `source /opt/Loris-MRI/bin/mri/environment`
- run `convert-dicom-archive-to-bids -h`
     => should print the help of the script. Make sure the help documentation is up-to-date.
- run `convert-dicom-archive-to-bids -p config.py -u <UPLOAD ID>` on a valid upload ID with fieldmaps and BOLD images
     => ensure that the `IntendedFor` field of the fieldmap has been updated to include the path to the BOLD images
     => once the script is done running, ensure that the temporary directory that was used to run the script has been cleared out
- run `convert-dicom-archive-to-bids -p config.py -u <UPLOAD ID> -s <SERIES UID>`
     => ensure only that the file(s) matching the `SeriesUID` have been ingested
- Bonus points: verify that the automated tests are still implemented

## Automated tests already implemented

- test invalid argument
- test invalid upload ID
- test invalid tarchive path
- test successful run on valid tarchive path
