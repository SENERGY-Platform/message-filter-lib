message-filter-lib
================

Library for filtering and extracting data from messages.

---

+ [Installation](#installation)
+ [Filters](#filters)
+ [FilterHandler](#filterhandler)
+ [FilterResult](#Filterresult)
+ [Builders](#builders)

---

## Installation

### Install

`pip install git+https://github.com/SENERGY-Platform/message-filter-lib.git@X.X.X`

Replace 'X.X.X' with the desired version.

### Upgrade

`pip install --upgrade git+https://github.com/SENERGY-Platform/message-filter-lib.git@X.X.X`

Replace 'X.X.X' with the desired version.

### Uninstall

`pip uninstall message-filter-lib`

## Filters

Filters are used to identify messages and extract data.
A filter is composed of an ID, a source from which the messages originate, mappings for data extraction as well as type conversion, optional message identifiers and filter arguments.
The latter can be any information that is necessary for the handling of extracted data.

The structure of a filter is shown below:

```python
{
    "id": "<filter id>",
    "source": "<message source>",
    "mappings": {
        "<target path>:<value type>:<mapping type>": "<source path>"
    },
    "identifiers": [
        {
            "key": "<message key name>",
            "value": "<message key value>"
        },
        {
            "key": "<message key name>"
        }
    ],
    "args": {}
}
```

### Mappings

A mappings are specified as a dictionary. A key consists of a target path under which data is to be extracted, a value type to which the data is to be converted and a mapping type.
The mapping types _data_ and _extra_ are available. The _data_ type defines which data will be extracted. 
Additional information that is relevant for handling the data, such as a timestamp, is extracted by the _extra_ type.
The source path to the message data to be extracted is specified as the value:

```python
{
    "<target path>:<value type>:<mapping type>": "<source path>"
}
```

### Identifiers

Identifiers allow messages to be identified by their content and structure. 
The use of identifiers makes it possible to differentiate messages and apply appropriate mappings.
This is relevant when messages with different structures originate from the same or multiple sources and an allocation via the source is not possible. 
Or messages with the same structure are to be distinguished by their content.
Identifiers are specified as a list of dictionaries. An identifier must have a "key" field and optionally a "value" field:

```python
[
    {
        "key": "<message key name>",
        "value": "<message key value>"
    },
    {
        "key": "<message key name>"
    }
]
```

The key field of an identifier specifies the name of a key that must be present in a message.
The Value field specifies a value for the key so that messages with the same data structures can be differentiated.
If no value field is used, the existence of the key referenced in the key field is sufficient for a message to be identified.

## FilterHandler

The FilterHandler class provides functionality for adding and removing filters as well as applying filters to messages and extracting data.

                 +---\        +---\                                                                    
                 |    ----+   |    ----+                                                               
                 | Filter |---| Filter |-\                                                             
                 |        |   |        |  \ +--------------+                              +----------+
                 +--------+   +--------+   >|              |   +---\        +---\         |          |
                                            |              |   |    ----+   |    ----+    |          |
                                            | FilterHandler|---|  Data  |---|  Data  |--->| Database |
                                            |              |   |        |   |        |    |          |
    +---\        +---\        +---\        >|              |   +--------+   +--------+    |          |
    |    ----+   |    ----+   |    ----+  / +--------------+                              +----------+
    | Message|---| Message|---| Message|-/                                                             
    |        |   |        |   |        |                                                               
    +--------+   +--------+   +--------+                                                               

### API

Create a FilterHandler object:

```python
mf_lib.filter.FilterHandler()
```

FilterHandler objects provide the following methods:

`add_filter(filter)`: Add a filter with the structure defined in [Filters](#filters). The _filter_ argument requires a dictionary.
Raises AddFilterError.

`delete_filter(id)`: Removes a filter by passing the ID of a filter as a string to the _id_ argument.
Raises DeleteFilterError.

`get_sources()`: Returns a list of strings containing all sources added by filters.

`get_filter_args(id)`: Returns a dictionary with filter arguments corresponding to the filter ID provided as a string to the _id_ argument.
Raises UnknownFilterIDError.

`get_results(message, source, data_builder, extra_builder)`: This method is used to apply filters by passing a message as a dictionary to the _message_ argument. 
Optionally, the source of the message can be passed as a string to the _source_ argument and custom [builders](#builders) to the _data_builder_ and _extra_builder_ arguments.
The method is a generator that and yields [FilterResult](#Filterresult) objects.
Raises NoFilterError.

## FilterResult

FilterResult objects store extracted data and additional information.

### API

`data`: The data extracted via the _data_ mapping type.

`extra`: Data extracted via the _extra_ mapping type.

`filter_ids`: IDs of filters used for data extraction.

`ex`: Any exception that occurred while applying the filters referenced in _filter_ids_. If an exception is present _data_ and _extra_ will be empty.

## Builders

Builder are functions that allow to customize the structure of extracted data according to the user's requirements.
Three builder functions are already provided by this repository:

### Dictionary builder

Stores data in a dictionary: `{"<key>": <value>, ...}`

### String list builder

Stores data as delimited key value strings in a list: `["<key>=<value>", ...]`

### Tuple list builder

Stores data as key value tuples in a list: `[(<key>, <value>), ...]`