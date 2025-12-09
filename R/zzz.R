#' @importFrom R6 R6Class
#' @importFrom methods is
NULL

.onAttach <- function(lib, pkg) {
  # Helper function to register resource resolvers
  registerResolver <- function(res) {
    # Get class name of resolver for status message
    class <- class(res)[[1]]

    # Display registration status message
    packageStartupMessage(paste0("Registering ", class, "..."))

    # Register the resolver
    resourcer::registerResourceResolver(res)
  }

  # Create and register Vault resolver on package attach
  registerResolver(VaultResourceResolver$new())
}

.onDetach <- function(lib) {
  # Clean up by unregistering the Vault resolver
  resourcer::unregisterResourceResolver("VaultResourceResolver")
}
