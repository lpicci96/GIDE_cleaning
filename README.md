# GIDE_cleaning

This script is for the dyadic variables in GIDE dataset. 
[Dataset found here](https://denveru.sharepoint.com/:x:/s/PardeeStaff/EeX3uD5OPbJBpMHKhaPEnTAB2iEv9LxiQslggfRB0S3gxw?e=KIN5XY)

Methodology:

- If values for mirrored pairs differ, take the value in alphabetical order of countrya and apply it to the mirrored value.
- If both values are null, ignore
- If only one value is null, replace value with the mirrored values (regardless of alphabetical order)

