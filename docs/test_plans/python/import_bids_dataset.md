# LORIS BIDS importer test plan

## Preamble

- The LORIS BIDS importer is a script to import a BIDS dataset in LORIS.
- Take a look at the documentation in the `loris-bids-importer` package README file.

## Testing instructions

### General

- Ensure that importing an incorrect path or non-BIDS file or directory returns an error.
- Ensure that the `--create-session` option creates a session in LORIS.
- Ensure that the `--create-candidate` option creates a candidate in LORIS.

### MRI

Import a BIDS dataset with MRI data.
- Ensure that the BIDS importer returns no unexpected error.
- Ensure that the imported MRI data is visible in the LORIS imaging browser, with brain browser visualization and preview pictures.
- Ensure that the imported MRI files are downloadlable in the LORIS imaging browser (NIfTI, sidecar JSON, BVAL and BVEC files)

### EEG

Import a BIDS dataset with EEG data.
- Ensure that the BIDS importer returns no unexpected error..
- Ensure that the imported EEG data is visible in the LORIS electrophysiology browser.
- Ensure that having the `useEEGBrowserVisualizationComponents` LORIS configuration option set to `1` or `true` during the import creates the electrophysiliogy chunk files, which allows to visualize the EEG signals int the LORIS electrophysiology browser after the import.
- Ensure that the imported EEG files are downloadlable in the LORIS imaging browser (acquisition file, sidecar JSON, event files, archives).

## Testing data

There is no data readily available for import for now. You can find use the Raisinbread data in the LORIS data directories `assembly_bids` (MRI) and `bids_imports` (Face13 dataset, EEG) if you remove them from LORIS before testing. You are encouraged to use your own datasets (such as public datasets) if you have some.
