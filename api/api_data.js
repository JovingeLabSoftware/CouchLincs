define({ "api": [
  {
    "type": "POST",
    "url": "/LINCS/pert",
    "title": "Create a perturbation data document",
    "name": "createPerturbation",
    "group": "LINCS",
    "description": "<p>Creates a data document for a particular perturbation &amp; calculation method</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "perturbagen",
            "description": "<p>name of perturbagen</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>numeric</p> ",
            "optional": false,
            "field": "dose",
            "description": "<p>dose (unitless)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>numeric</p> ",
            "optional": false,
            "field": "duration",
            "description": "<p>duration (unitless)</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "cell",
            "description": "<p>cell line used</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "method",
            "description": "<p>calculation method used, e.g. &quot;zsvc_plate&quot;</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>boolean</p> ",
            "optional": false,
            "field": "gold",
            "description": "<p>is this a gold signature score?</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[String]</p> ",
            "optional": false,
            "field": "gene_ids",
            "description": "<p>from lincs</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[Numeric]</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>the scores (one per gene)</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "Id",
            "description": "<p>of created object</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 201 Created\n{\n id: 1\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/instances/distil_id",
    "title": "Request data by distil_id (multi)",
    "name": "distilIdsData",
    "group": "LINCS",
    "description": "<p>Fetch metadata for given instance by distil_id.  We use POST here because keys are long and could quickly exceed GET query string limit imposed by many clients.</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>string[]</p> ",
            "optional": false,
            "field": "ids",
            "description": "<p>distilIds of instances</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>string[]</p> ",
            "optional": false,
            "field": "fields",
            "description": "<p>fields to return (defaults to all data)</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl -d '{\"ids\": [\"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05\", \\\n      \"CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05\"]}' \\\n      localhost:8080/LINCS/instances/distil_id \\\n      -H \"Content-Type: application/json\"",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>docs in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n[  \n{\n   \"data\": { ... }\n   \"gene_ids\": { ... }\n   \"metadata\": { ... },\n   \"id\": \"312240\"\n } \n... // truncated\n]",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/:id/controls",
    "title": "Retrieve control data",
    "name": "getControls",
    "group": "LINCS",
    "description": "<p>Retrieves the full data document for each instance on the same plate as :id whose perturbagen is the appropriate control for that instance (including time of exposure and cell line).</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "id",
            "description": "<p>of instance for which controls are desired.</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>Document in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response (note array): ",
          "content": "HTTP/1.1 200 OK\n{\n[\"gene_ids\": [\n    \"200814_at\",\n    \"222103_at\",\n    \"...truncated...\"\n    ],\n\"metadata\": {\n    \"bead_batch\": \"b3\",\n    \"bead_revision\": \"r2\",\n    \"bead_set\": \"dp52,dp53\",\n    \"cell_id\": \"HCC515\",\n    \"...truncated...\"\n},\n\"data\": [\n    9.15469932556152,\n    9.05399990081787,\n    \"...truncated...\"\n    ]\n },\n \"...truncated...\"\n]\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/sh_controls",
    "title": "Retrieve control data for shRNA",
    "name": "getShControls",
    "group": "LINCS",
    "description": "<p>Retrieves the appropriate shRNA controls</p> ",
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/distil_id/:distil_id",
    "title": "Request instance by distil_id",
    "name": "instanceByDistil_id",
    "group": "LINCS",
    "description": "<p>Fetch data for a given instance by distil_id</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "distil_id",
            "description": "<p>Id of instance</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl localhost:8080/LINCS/instances/\\\n   CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>Data doc in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n{\n  \"data\": { ... },\n  \"gene_ids\": { ... },\n  \"metadata\": { ... },\n  \"id\": \"1230884\"\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/distil_id/:distil_id/:fields",
    "title": "Request instance by distil_id",
    "name": "instanceByDistil_idWithField",
    "group": "LINCS",
    "description": "<p>Fetch specific field of data for a given instance by distil_id</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "distil_id",
            "description": "<p>Id of instance</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "fields",
            "description": "<p>optional list of fields in JSON format (e.g. {fields = [metadata.pert_desc, metadata.pert_id]} to limit data returned.  Default is all data.</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl localhost:8080/LINCS/instances/\\\n   CPC014_VCAP_6H_X2_F1B3_DUO52HI53LO:P05/metadata",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "metadata",
            "description": "<p>Metadats docs in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n{\n  \"data\": { ... },\n  \"metadata\": { ... },\n  \"gene_ids\": { ... },\n  \"id\": \"1230884\"\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances/:id",
    "title": "Request instance by id",
    "name": "instanceById",
    "group": "LINCS",
    "description": "<p>Fetch data for a given instance by id</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "id",
            "description": "<p>Id of instance</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl localhost:8080/LINCS/instances/2",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>data docs in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n{\n  \"data\": { ... },\n  \"metadata\": { ... },\n  \"timestamp\": \"2015-12-18 03:52:52\",\n  \"id\": \"1230884\"\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "GET",
    "url": "/LINCS/instances",
    "title": "Query instances",
    "name": "instanceQuery",
    "group": "LINCS",
    "description": "<p>Fetch data for instances matching query</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String[]</p> ",
            "optional": true,
            "field": "ids",
            "description": "<p>Optional explicit list of primary ids</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String[]</p> ",
            "optional": true,
            "field": "fields",
            "description": "<p>Optional list of fields to return, e.g. [metadata.pert_desc, gene_ids]</p> "
          }
        ]
      }
    },
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl -G \"http://localhost:8085/LINCS/instances\" --data-urlencode \"ids=[1,2,3]\" --data-urlencode fields='[\"metadata.pert_type\"]'",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>Object</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>data docs in JSON format</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nContent-Type: application/json\n{\n  [\n    {\n      \"id\":\"1\",\n      \"pert_type\":\"trt_cp\"\n      },\n      {\n         \"id\":\"2\",\n         \"pert_type\":\"trt_cp\"\n          \n      },\n      {\n        \"id\":\"3\",\n        \"pert_type\":\"trt_cp\"\n      }\n    ]\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/instances",
    "title": "Save instance data to server",
    "name": "postInstance",
    "group": "LINCS",
    "description": "<p>Stores instance (e.g. level 2 data) on the server. Do NOT use this endpoint to save calculated scores (use /LINCS/pert for that).</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "id",
            "description": "<p>desired id (key) of document</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "metadata",
            "description": "<p>JSON from lincs</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "type",
            "description": "<p>Type of data, e.g. &quot;q2norm&quot;</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[String]</p> ",
            "optional": false,
            "field": "gene_ids",
            "description": "<p>from lincs</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>[Numeric]</p> ",
            "optional": false,
            "field": "data",
            "description": "<p>the expression data</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "response",
            "description": "<p>response</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "POST",
    "url": "/LINCS/data Post ids (either primary or view) and retrieve",
    "title": "documents",
    "name": "retrieveData",
    "group": "LINCS",
    "description": "<p>Retrieves multiple docs by primary or view id.  We use POST here becuase keys are large and quickly exceed GET query string length limit imposed by many clients</p> ",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "<p>[String]</p> ",
            "optional": false,
            "field": "keys",
            "description": "<p>List of primary or view keys</p> "
          },
          {
            "group": "Parameter",
            "type": "<p>String</p> ",
            "optional": false,
            "field": "view",
            "description": "<p>Name of view.  If missing will use primary index</p> "
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "Id",
            "description": "<p>of created object</p> "
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\n{\n ...\n}",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "LINCS"
  },
  {
    "type": "get",
    "url": "/LINCS",
    "title": "Get some documentation on endpoints.",
    "name": "Root",
    "group": "Root",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "response",
            "description": "<p>Some useful information.</p> "
          }
        ]
      }
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "Root"
  },
  {
    "type": "get",
    "url": "/",
    "title": "Root.  Verify server up and receive useful advice.",
    "name": "Root",
    "group": "Root",
    "examples": [
      {
        "title": "Example usage:",
        "content": "curl http://localhost:8080/",
        "type": "curl"
      }
    ],
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "<p>string</p> ",
            "optional": false,
            "field": "response",
            "description": ""
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response: ",
          "content": "HTTP/1.1 200 OK\nOK.  Send GET to /LINCS for further documentation.",
          "type": "json"
        }
      ]
    },
    "version": "0.0.0",
    "filename": "bin/app.js",
    "groupTitle": "Root"
  }
] });