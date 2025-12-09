var dsVault = {
  settings: {
    title: "DataSHIELD Vault Resources",
    description:
      "Provides access to DataSHIELD Vault collections for secure medical data storage.",
    web: "https://github.com/isglobal-brge/dsVault",
    categories: [
      {
        name: "dsvault-collection",
        title: "dsVault Collection",
        description:
          'The resource connects to a <a href="https://github.com/isglobal-brge/dsVault" target="_blank">DataSHIELD Vault</a> collection.',
      },
    ],
    types: [
      {
        name: "dsvault-collection",
        title: "DataSHIELD Vault Collection",
        description:
          'Connection to a DataSHIELD Vault collection containing medical images or other data.',
        tags: ["dsvault-collection"],
        parameters: {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "host",
              "type": "string",
              "title": "Host",
              "description": "The hostname or IP address of the Vault API"
            },
            {
              "key": "port",
              "type": "integer",
              "title": "Port",
              "description": "The port number of the Vault API"
            },
            {
              "key": "collection",
              "type": "string",
              "title": "Collection Name",
              "description": "The name of the collection in the vault"
            }
          ],
          "required": ["host", "port", "collection"],
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "apikey",
              "type": "string",
              "title": "Vault Key",
              "format": "password",
              "description": "The API key for accessing the collection"
            }
          ],
          "required": ["apikey"],
        },
      },
    ],
  },
  asResource: function (type, name, params, credentials) {
    var toVaultResource = function(name, params, credentials) {
      return {
        name: name,
        url: "http://" + params.host + ":" + params.port + "/collection/" + params.collection,
        format: "dsvault.collection",
        identity: params.collection,
        secret: credentials.apikey
      };
    };

    var toResourceFactories = {
      "dsvault-collection": toVaultResource
    };

    if (toResourceFactories[type]) {
      return toResourceFactories[type](name, params, credentials);
    }
    return undefined;
  },
};
