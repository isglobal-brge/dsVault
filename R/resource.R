#' Vault Collection Resource Client
#'
#' This R6 class manages connections to DataSHIELD Vault collections.
#' It extends the generic ResourceClient class to provide dsVault-specific functionality.
#'
#' @description
#' The VaultResourceClient class provides methods to:
#' * Initialize connections with dsVault collections
#' * Access collection methods (list objects, get hashes, download)
#'
#' @details
#' The class uses a URL-based configuration system with the format:
#' `http://host:port/collection/collection_name`
#'
#' @examples
#' \dontrun{
#' # Create a resource object (as provided by Opal)
#' resource <- list(
#'   url = "http://localhost:8000/collection/my-collection",
#'   format = "dsvault.collection",
#'   identity = "my-collection",
#'   secret = "my-api-key"
#' )
#'
#' # Initialize client
#' client <- VaultResourceClient$new(resource)
#'
#' # Get the DSVaultCollection object
#' collection <- client$getConnection()
#' }
#'
#' @importFrom R6 R6Class
#' @export
VaultResourceClient <- R6::R6Class(
  "VaultResourceClient",
  inherit = resourcer::ResourceClient,
  public = list(
    #' @description
    #' Initialize a new VaultResourceClient instance
    #'
    #' @param resource A list containing resource configuration (url, format, identity, secret)
    #'
    #' @return A new VaultResourceClient object
    initialize = function(resource) {
      # Check if resource is valid for this client
      if (!is.list(resource) || is.null(resource$format)) {
        stop("Invalid resource: must have a 'format' field")
      }

      # Get the list of expected formats
      expected_formats <- private$.expected_formats

      # Check if format is acceptable
      if (!tolower(resource$format) %in% expected_formats) {
        stop(paste0("Invalid resource format: '", resource$format,
                   "'. Expected one of: ", paste(expected_formats, collapse=", ")))
      }

      # Initialize parent class
      super$initialize(resource)
    },

    #' @description
    #' Get a DSVaultCollection connection
    #'
    #' @return A DSVaultCollection object
    getConnection = function() {
      if (is.null(private$.collection)) {
        private$.collection <- private$.createCollection()
      }
      return(private$.collection)
    },

    #' @description
    #' Get the resource value (DSVaultCollection object)
    #' This method is called by resourcer::as.resource.object()
    #'
    #' @param ... Additional arguments (ignored)
    #' @return A DSVaultCollection object
    getValue = function(...) {
      self$getConnection()
    },

    #' @description
    #' List all objects in the collection
    #'
    #' @return Character vector of object names
    listObjects = function() {
      collection <- self$getConnection()
      collection$list_objects()
    },

    #' @description
    #' Get hashes of all objects in the collection
    #'
    #' @return Data frame with columns: name, hash_sha256
    listHashes = function() {
      collection <- self$getConnection()
      collection$list_hashes()
    },

    #' @description
    #' Get the SHA-256 hash of a specific object
    #'
    #' @param name Name of the object
    #'
    #' @return Character string with the SHA-256 hash
    getHash = function(name) {
      collection <- self$getConnection()
      collection$get_hash(name)
    },

    #' @description
    #' Download an object from the collection into memory
    #'
    #' @param name Name of the object
    #'
    #' @return Raw vector with the object content
    download = function(name) {
      collection <- self$getConnection()
      collection$download(name)
    }
  ),

  private = list(
    #' List of acceptable resource formats for this client
    .expected_formats = c("dsvault.collection"),

    #' Cached DSVaultCollection object
    .collection = NULL,

    #' Create DSVaultCollection from resource
    #'
    #' This method creates a DSVaultCollection object using the resource information
    .createCollection = function() {
      resource <- super$getResource()

      # Parse URL to extract endpoint
      # URL format: http://host:port/collection/collection_name
      url <- resource$url

      # Extract everything before /collection/
      endpoint <- sub("/collection/.*$", "", url)

      # Get collection name from identity field (set by resource.js)
      collection_name <- resource$identity

      # Get API key from secret
      api_key <- resource$secret

      # Create and return DSVaultCollection
      DSVaultCollection$new(
        endpoint = endpoint,
        collection = collection_name,
        api_key = api_key
      )
    }
  )
)

#' Vault Collection Resource Resolver
#'
#' This R6 class handles resolution of dsVault collection resources. It validates
#' resource configurations and creates appropriate client instances for interacting
#' with DataSHIELD Vault collections.
#'
#' @description
#' The resolver performs two main functions:
#' 1. Validates if a resource configuration is suitable for dsVault collections
#' 2. Creates new VaultResourceClient instances for valid resources
#'
#' @examples
#' \dontrun{
#' resolver <- VaultResourceResolver$new()
#'
#' # Check if resource is suitable
#' resource <- list(
#'   url = "http://localhost:8000/collection/my-collection",
#'   format = "dsvault.collection"
#' )
#' is_suitable <- resolver$isFor(resource)
#'
#' # Create client if suitable
#' client <- resolver$newClient(resource)
#' }
#'
#' @export
VaultResourceResolver <- R6::R6Class(
  "VaultResourceResolver",
  inherit = resourcer::ResourceResolver,
  public = list(
    #' @description
    #' Check if a resource is suitable for dsVault collection handling
    #'
    #' @param resource Resource configuration to validate
    #' @return Logical indicating if resource is suitable
    isFor = function(resource) {
      isSuitable <- super$isFor(resource) &&
        tolower(resource$format) %in% c("dsvault.collection")
      return(isSuitable)
    },

    #' @description
    #' Create new client for dsVault collection resource
    #'
    #' @param resource Resource configuration
    #' @return New VaultResourceClient instance or NULL if resource unsuitable
    newClient = function(resource) {
      if (self$isFor(resource)) {
        client <- VaultResourceClient$new(resource)
        return(client)
      } else {
        return(NULL)
      }
    }
  )
)
