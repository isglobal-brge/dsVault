#' DataSHIELD Vault Collection
#'
#' R6 class for interacting with a DataSHIELD Vault collection.
#'
#' @export
#' @importFrom R6 R6Class
#' @importFrom httr2 request req_headers req_perform resp_body_json resp_body_raw
#'
#' @examples
#' \dontrun{
#' vault <- DSVaultCollection$new(
#'   endpoint = "http://localhost:8000",
#'   collection = "my-collection",
#'   api_key = "my-api-key"
#' )
#'
#' vault$list_objects()
#' vault$list_hashes()
#' vault$get_hash("file.csv")
#' content <- vault$download("file.csv")  # returns raw bytes in memory
#' }
DSVaultCollection <- R6::R6Class(
  classname = "DSVaultCollection",

  public = list(
    #' @field endpoint Base URL of the vault API.
    endpoint = NULL,

    #' @field collection Name of the collection.
    collection = NULL,

    #' @description
    #' Create a new DataSHIELD Vault Collection client.
    #'
    #' @param endpoint Base URL of the vault API (e.g., "http://localhost:8000").
    #' @param collection Name of the collection.
    #' @param api_key API key for the collection.
    #'
    #' @return A new `DSVaultCollection` object.
    initialize = function(endpoint, collection, api_key) {
      self$endpoint <- sub("/$", "", endpoint)
      self$collection <- collection
      private$.api_key <- api_key
      private$.base_url <- paste0(self$endpoint, "/api/v1/collections/", self$collection)
    },

    #' @description
    #' List all objects in the collection.
    #'
    #' @return Character vector of object names.
    list_objects = function() {
      resp <- private$request("/objects")
      unlist(resp$objects)
    },

    #' @description
    #' Get hashes of all objects in the collection.
    #'
    #' @return Data frame with columns: name, hash_sha256.
    list_hashes = function() {
      resp <- private$request("/hashes")
      if (length(resp$items) == 0) {
        return(data.frame(name = character(), hash_sha256 = character()))
      }
      do.call(rbind, lapply(resp$items, as.data.frame))
    },

    #' @description
    #' Get the SHA-256 hash of a specific object.
    #'
    #' @param name Name of the object.
    #'
    #' @return Character string with the SHA-256 hash.
    get_hash = function(name) {
      resp <- private$request(paste0("/hashes/", name))
      resp$hash_sha256
    },

    #' @description
    #' Download an object from the collection into memory.
    #'
    #' @param name Name of the object.
    #'
    #' @return Raw vector with the object content.
    download = function(name) {
      url <- paste0(private$.base_url, "/objects/", name)

      req <- httr2::request(url) |>
        httr2::req_headers(`X-Collection-Key` = private$.api_key)

      resp <- httr2::req_perform(req)
      httr2::resp_body_raw(resp)
    },

    #' @description
    #' Print method for DataSHIELD Vault Collection object.
    print = function() {
      cat("DataSHIELD Vault Collection\n")
      cat("  Collection:", self$collection, "\n")
      cat("  Endpoint:", self$endpoint, "\n")
      invisible(self)
    }
  ),

  private = list(
    .api_key = NULL,
    .base_url = NULL,

    request = function(path) {
      url <- paste0(private$.base_url, path)

      req <- httr2::request(url) |>
        httr2::req_headers(`X-Collection-Key` = private$.api_key)

      resp <- httr2::req_perform(req)
      httr2::resp_body_json(resp)
    }
  )
)
