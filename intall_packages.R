# Check the version of the vctrs package


#vctrs_version <- packageVersion("vctrs")
#print(vctrs_version)

# Ensure the 'remotes' package is installed
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

# Install a specific version of 'vctrs'
remotes::install_version("vctrs", version = "0.6.4")

# Now, try loading the 'sparklyr' package again
#install.packages("vctrs")
install.packages("sparklyr")

install.packages("forecast")
install.packages("lubridate")
install.packages("arrow")