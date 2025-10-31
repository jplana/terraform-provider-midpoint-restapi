package restapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
)

type apiObjectOpts struct {
	path          string
	getPath       string
	postPath      string
	putPath       string
	createMethod  string
	readMethod    string
	readData      string
	updateMethod  string
	updateData    string
	destroyMethod string
	destroyData   string
	deletePath    string
	searchPath    string
	queryString   string
	debug         bool
	readSearch    map[string]string
	id            string
	idAttribute   string
	data          string
}

/*APIObject is the state holding struct for a restapi_object resource*/
type APIObject struct {
	apiClient     *APIClient
	getPath       string
	postPath      string
	putPath       string
	createMethod  string
	readMethod    string
	updateMethod  string
	destroyMethod string
	deletePath    string
	searchPath    string
	queryString   string
	debug         bool
	readSearch    map[string]string
	id            string
	idAttribute   string

	/* Set internally */
	data            map[string]interface{} /* Data as managed by the user */
	readData        map[string]interface{} /* Read data as managed by the user */
	updateData      map[string]interface{} /* Update data as managed by the user */
	destroyData     map[string]interface{} /* Destroy data as managed by the user */
	apiData         map[string]interface{} /* Data as available from the API */
	apiResponse     string
	ignoreChangesTo []string /* Fields to ignore when detecting changes */
}

// NewAPIObject makes an APIobject to manage a RESTful object in an API
func NewAPIObject(iClient *APIClient, opts *apiObjectOpts) (*APIObject, error) {
	if opts.debug {
		log.Printf("api_object.go: Constructing debug api_object\n")
		log.Printf(" id: %s\n", opts.id)
	}

	/* id_attribute can be set either on the client (to apply for all calls with the server)
	   or on a per object basis (for only calls to this kind of object).
	   Permit overridding from the API client here by using the client-wide value only
	   if a per-object value is not set */
	if opts.idAttribute == "" {
		opts.idAttribute = iClient.idAttribute
	}

	if opts.createMethod == "" {
		opts.createMethod = iClient.createMethod
	}
	if opts.readMethod == "" {
		opts.readMethod = iClient.readMethod
	}
	if opts.readData == "" {
		opts.readData = iClient.readData
	}
	if opts.updateMethod == "" {
		opts.updateMethod = iClient.updateMethod
	}
	if opts.updateData == "" {
		opts.updateData = iClient.updateData
	}
	if opts.destroyMethod == "" {
		opts.destroyMethod = iClient.destroyMethod
	}
	if opts.destroyData == "" {
		opts.destroyData = iClient.destroyData
	}
	if opts.postPath == "" {
		opts.postPath = opts.path
	}
	if opts.getPath == "" {
		opts.getPath = opts.path + "/{id}"
	}
	if opts.putPath == "" {
		opts.putPath = opts.path + "/{id}"
	}
	if opts.deletePath == "" {
		opts.deletePath = opts.path + "/{id}"
	}
	if opts.searchPath == "" {
		opts.searchPath = opts.path
	}

	obj := APIObject{
		apiClient:     iClient,
		getPath:       opts.getPath,
		postPath:      opts.postPath,
		putPath:       opts.putPath,
		createMethod:  opts.createMethod,
		readMethod:    opts.readMethod,
		updateMethod:  opts.updateMethod,
		destroyMethod: opts.destroyMethod,
		deletePath:    opts.deletePath,
		searchPath:    opts.searchPath,
		queryString:   opts.queryString,
		debug:         opts.debug,
		readSearch:    opts.readSearch,
		id:            opts.id,
		idAttribute:   opts.idAttribute,
		data:          make(map[string]interface{}),
		readData:      make(map[string]interface{}),
		updateData:    make(map[string]interface{}),
		destroyData:   make(map[string]interface{}),
		apiData:       make(map[string]interface{}),
	}

	if opts.data != "" {
		if opts.debug {
			log.Printf("api_object.go: Parsing data: '%s'", opts.data)
		}

		err := json.Unmarshal([]byte(opts.data), &obj.data)
		if err != nil {
			return &obj, fmt.Errorf("api_object.go: error parsing data provided: %v", err.Error())
		}

		/* Opportunistically set the object's ID if it is provided in the data.
		   If it is not set, we will get it later in synchronize_state */
		if obj.id == "" {
			var tmp string
			tmp, err := GetStringAtKey(obj.data, obj.idAttribute, obj.debug)
			if err == nil {
				if opts.debug {
					log.Printf("api_object.go: opportunisticly set id from data provided.")
				}
				obj.id = tmp
			} else if !obj.apiClient.writeReturnsObject && !obj.apiClient.createReturnsObject && obj.searchPath == "" {
				/* If the id is not set and we cannot obtain it
				   later, error out to be safe */
				return &obj, fmt.Errorf("provided data does not have %s attribute for the object's id and the client is not configured to read the object from a POST response; without an id, the object cannot be managed", obj.idAttribute)
			}
		}
	}

	if opts.readData != "" {
		if opts.debug {
			log.Printf("api_object.go: Parsing read data: '%s'", opts.readData)
		}

		err := json.Unmarshal([]byte(opts.readData), &obj.readData)
		if err != nil {
			return &obj, fmt.Errorf("api_object.go: error parsing read data provided: %v", err.Error())
		}
	}

	if opts.updateData != "" {
		if opts.debug {
			log.Printf("api_object.go: Parsing update data: '%s'", opts.updateData)
		}

		err := json.Unmarshal([]byte(opts.updateData), &obj.updateData)
		if err != nil {
			return &obj, fmt.Errorf("api_object.go: error parsing update data provided: %v", err.Error())
		}
	}

	if opts.destroyData != "" {
		if opts.debug {
			log.Printf("api_object.go: Parsing destroy data: '%s'", opts.destroyData)
		}

		err := json.Unmarshal([]byte(opts.destroyData), &obj.destroyData)
		if err != nil {
			return &obj, fmt.Errorf("api_object.go: error parsing destroy data provided: %v", err.Error())
		}
	}

	if opts.debug {
		log.Printf("api_object.go: Constructed object: %s", obj.toString())
	}
	return &obj, nil
}

// Convert the important bits about this object to string representation
// This is useful for debugging.
func (obj *APIObject) toString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("id: %s\n", obj.id))
	buffer.WriteString(fmt.Sprintf("get_path: %s\n", obj.getPath))
	buffer.WriteString(fmt.Sprintf("post_path: %s\n", obj.postPath))
	buffer.WriteString(fmt.Sprintf("put_path: %s\n", obj.putPath))
	buffer.WriteString(fmt.Sprintf("delete_path: %s\n", obj.deletePath))
	buffer.WriteString(fmt.Sprintf("query_string: %s\n", obj.queryString))
	buffer.WriteString(fmt.Sprintf("create_method: %s\n", obj.createMethod))
	buffer.WriteString(fmt.Sprintf("read_method: %s\n", obj.readMethod))
	buffer.WriteString(fmt.Sprintf("update_method: %s\n", obj.updateMethod))
	buffer.WriteString(fmt.Sprintf("destroy_method: %s\n", obj.destroyMethod))
	buffer.WriteString(fmt.Sprintf("debug: %t\n", obj.debug))
	buffer.WriteString(fmt.Sprintf("read_search: %s\n", spew.Sdump(obj.readSearch)))
	buffer.WriteString(fmt.Sprintf("data: %s\n", spew.Sdump(obj.data)))
	buffer.WriteString(fmt.Sprintf("read_data: %s\n", spew.Sdump(obj.readData)))
	buffer.WriteString(fmt.Sprintf("update_data: %s\n", spew.Sdump(obj.updateData)))
	buffer.WriteString(fmt.Sprintf("destroy_data: %s\n", spew.Sdump(obj.destroyData)))
	buffer.WriteString(fmt.Sprintf("api_data: %s\n", spew.Sdump(obj.apiData)))
	return buffer.String()
}

/*
Centralized function to ensure that our data as managed by

	the api_object is updated with data that has come back from
	the API
*/
func (obj *APIObject) updateState(state string) error {
	if obj.debug {
		log.Printf("api_object.go: Updating API object state to '%s'\n", state)
	}

	/* Other option - Decode as JSON Numbers instead of golang datatypes
	d := json.NewDecoder(strings.NewReader(res_str))
	d.UseNumber()
	err = d.Decode(&obj.api_data)
	*/
	err := json.Unmarshal([]byte(state), &obj.apiData)
	if err != nil {
		return err
	}

	/* Store response body for parsing via jsondecode() */
	obj.apiResponse = state

	/* A usable ID was not passed (in constructor or here),
	   so we have to guess what it is from the data structure */
	if obj.id == "" {
		val, err := GetStringAtKey(obj.apiData, obj.idAttribute, obj.debug)
		if err != nil {
			return fmt.Errorf("api_object.go: Error extracting ID from data element: %s", err)
		}
		obj.id = val
	} else if obj.debug {
		log.Printf("api_object.go: Not updating id. It is already set to '%s'\n", obj.id)
	}

	/* Any keys that come from the data we want to copy are done here */
	if len(obj.apiClient.copyKeys) > 0 {
		for _, key := range obj.apiClient.copyKeys {
			if obj.debug {
				log.Printf("api_object.go: Copying key '%s' from api_data (%v) to data (%v)\n", key, obj.apiData[key], obj.data[key])
			}
			obj.data[key] = obj.apiData[key]
		}
	} else if obj.debug {
		log.Printf("api_object.go: copy_keys is empty - not attempting to copy data")
	}

	if obj.debug {
		log.Printf("api_object.go: final object after synchronization of state:\n%+v\n", obj.toString())
	}
	return err
}

func (obj *APIObject) createObject() error {
	/* Failsafe: The constructor should prevent this situation, but
	   protect here also. If no id is set, and the API does not respond
	   with the id of whatever gets created, we have no way to know what
	   the object's id will be. Abandon this attempt */
	if obj.id == "" && !obj.apiClient.writeReturnsObject && !obj.apiClient.createReturnsObject {
		return fmt.Errorf("provided object does not have an id set and the client is not configured to read the object from a POST or PUT response; please set write_returns_object to true, or include an id in the object's data")
	}

	// Filter ignored fields from the data before sending
	dataToSend := obj.data
	if len(obj.ignoreChangesTo) > 0 {
		dataToSend = filterIgnoredFields(obj.data, obj.ignoreChangesTo)
		if obj.debug {
			log.Printf("api_object.go: Filtered ignored fields for CREATE operation")
		}
	}

	b, _ := json.Marshal(dataToSend)

	postPath := obj.postPath
	if obj.queryString != "" {
		if obj.debug {
			log.Printf("api_object.go: Adding query string '%s'", obj.queryString)
		}
		postPath = fmt.Sprintf("%s?%s", obj.postPath, obj.queryString)
	}

	resultString, err := obj.apiClient.sendRequest(obj.createMethod, strings.Replace(postPath, "{id}", obj.id, -1), string(b))
	if err != nil {
		return err
	}

	/* We will need to sync state as well as get the object's ID */
	if obj.apiClient.writeReturnsObject || obj.apiClient.createReturnsObject {
		if obj.debug {
			log.Printf("api_object.go: Parsing response from POST to update internal structures (write_returns_object=%t, create_returns_object=%t)...\n",
				obj.apiClient.writeReturnsObject, obj.apiClient.createReturnsObject)
		}
		err = obj.updateState(resultString)
		/* Yet another failsafe. In case something terrible went wrong internally,
		   bail out so the user at least knows that the ID did not get set. */
		if obj.id == "" {
			return fmt.Errorf("internal validation failed; object ID is not set, but *may* have been created; this should never happen")
		}
	} else {
		if obj.debug {
			log.Printf("api_object.go: Requesting created object from API (write_returns_object=%t, create_returns_object=%t)...\n",
				obj.apiClient.writeReturnsObject, obj.apiClient.createReturnsObject)
		}
		err = obj.readObject()
	}
	return err
}

func (obj *APIObject) readObject() error {
	if obj.id == "" {
		return fmt.Errorf("cannot read an object unless the ID has been set")
	}

	getPath := obj.getPath
	if obj.queryString != "" {
		if obj.debug {
			log.Printf("api_object.go: Adding query string '%s'", obj.queryString)
		}
		getPath = fmt.Sprintf("%s?%s", obj.getPath, obj.queryString)
	}

	send := ""
	if len(obj.readData) > 0 {
		readData, _ := json.Marshal(obj.readData)
		send = string(readData)
		if obj.debug {
			log.Printf("api_object.go: Using read data '%s'", send)
		}
	}

	resultString, err := obj.apiClient.sendRequest(obj.readMethod, strings.Replace(getPath, "{id}", obj.id, -1), send)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected response code '404'") {
			log.Printf("api_object.go: 404 error while refreshing state for '%s' at path '%s'. Removing from state.", obj.id, obj.getPath)
			obj.id = ""
			return nil
		}
		return err
	}

	searchKey := obj.readSearch["search_key"]
	searchValue := obj.readSearch["search_value"]

	if searchKey != "" && searchValue != "" {

		obj.searchPath = strings.Replace(obj.getPath, "{id}", obj.id, -1)

		queryString := obj.readSearch["query_string"]
		if obj.queryString != "" {
			if obj.debug {
				log.Printf("api_object.go: Adding query string '%s'", obj.queryString)
			}
			queryString = fmt.Sprintf("%s&%s", obj.readSearch["query_string"], obj.queryString)
		}
		searchData := ""
		if len(obj.readSearch["search_data"]) > 0 {
			tmpData, _ := json.Marshal(obj.readSearch["search_data"])
			searchData = string(tmpData)
			if obj.debug {
				log.Printf("api_object.go: Using search data '%s'", searchData)
			}
		}

		resultsKey := obj.readSearch["results_key"]
		objFound, err := obj.findObject(queryString, searchKey, searchValue, resultsKey, searchData)
		if err != nil || objFound == nil {
			log.Printf("api_object.go: Search did not find object with the '%s' key = '%s'", searchKey, searchValue)
			obj.id = ""
			return nil
		}
		objFoundString, _ := json.Marshal(objFound)
		return obj.updateState(string(objFoundString))
	}

	return obj.updateState(resultString)
}

func (obj *APIObject) updateObject() error {
	if obj.id == "" {
		return fmt.Errorf("cannot update an object unless the ID has been set")
	}

	// Write debug log
	debugFile := "/tmp/midpoint-patch-debug.log"
	f, _ := os.OpenFile(debugFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("\n===== updateObject called %s =====\n", time.Now().Format("2006-01-02 15:04:05")))
		f.WriteString(fmt.Sprintf("Object ID: %s\n", obj.id))
		f.WriteString(fmt.Sprintf("Update method: %s\n", obj.apiClient.updateMethod))
		f.Close()
	}

	// For Midpoint integration, send the object via PATCH
	if obj.updateMethod == "PATCH" {
		// Write debug log
		f, _ := os.OpenFile(debugFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("Using PATCH method\n")
			f.WriteString("Calling readObject()...\n")
			f.Close()
		}

		// First, fetch current state to compare with desired state
		err := obj.readObject()
		if err != nil {
			return fmt.Errorf("failed to read object for PATCH operation: %v", err)
		}

		// Write debug log
		f, _ = os.OpenFile(debugFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("readObject() completed successfully\n")
			f.WriteString("Calling patchMidpointObject()...\n")
			f.Close()
		}

		// We have apiData (current) and obj.data (desired)
		// Now calculate what changed and form appropriate PATCH requests
		return obj.patchMidpointObject()
	}

	// Original PUT behavior
	send := ""
	if len(obj.updateData) > 0 {
		updateData, _ := json.Marshal(obj.updateData)
		send = string(updateData)
		if obj.debug {
			log.Printf("api_object.go: Using update data '%s'", send)
		}
	} else {
		// Filter ignored fields from the data before sending
		dataToSend := obj.data
		if len(obj.ignoreChangesTo) > 0 {
			dataToSend = filterIgnoredFields(obj.data, obj.ignoreChangesTo)
			if obj.debug {
				log.Printf("api_object.go: Filtered ignored fields for UPDATE operation")
			}
		}
		b, _ := json.Marshal(dataToSend)
		send = string(b)
	}

	putPath := obj.putPath
	if obj.queryString != "" {
		if obj.debug {
			log.Printf("api_object.go: Adding query string '%s'", obj.queryString)
		}
		putPath = fmt.Sprintf("%s?%s", obj.putPath, obj.queryString)
	}

	resultString, err := obj.apiClient.sendRequest(obj.updateMethod, strings.Replace(putPath, "{id}", obj.id, -1), send)
	if err != nil {
		return err
	}

	if obj.apiClient.writeReturnsObject {
		if obj.debug {
			log.Printf("api_object.go: Parsing response from PUT to update internal structures (write_returns_object=true)...\n")
		}
		err = obj.updateState(resultString)
	} else {
		if obj.debug {
			log.Printf("api_object.go: Requesting updated object from API (write_returns_object=false)...\n")
		}
		err = obj.readObject()
	}
	return err
}

func (obj *APIObject) deleteObject() error {
	if obj.id == "" {
		log.Printf("WARNING: Attempting to delete an object that has no id set. Assuming this is OK.\n")
		return nil
	}

	deletePath := obj.deletePath
	if obj.queryString != "" {
		if obj.debug {
			log.Printf("api_object.go: Adding query string '%s'", obj.queryString)
		}
		deletePath = fmt.Sprintf("%s?%s", obj.deletePath, obj.queryString)
	}

	send := ""
	if len(obj.destroyData) > 0 {
		destroyData, _ := json.Marshal(obj.destroyData)
		send = string(destroyData)
		if obj.debug {
			log.Printf("api_object.go: Using destroy data '%s'", string(destroyData))
		}
	}

	_, err := obj.apiClient.sendRequest(obj.destroyMethod, strings.Replace(deletePath, "{id}", obj.id, -1), send)
	if err != nil {
		return err
	}

	return nil
}

// patchMidpointObject calculates differences between current and desired state
// and makes PATCH requests for each modification needed using Midpoint's ObjectModificationType format
/*
 * mergeIgnoredFields recursively merges ignored fields from API data into desired data.
 * This ensures that server-managed fields are preserved during PATCH operations,
 * even when they're nested deeply within objects.
 */
func mergeIgnoredFields(desired, api map[string]interface{}, ignoreList []string, debug bool) map[string]interface{} {
	result := make(map[string]interface{})

	// Start with all desired fields
	for k, v := range desired {
		result[k] = v
	}

	// Merge ignored fields from API
	for key, apiValue := range api {
		// Check if this key matches an ignore pattern at the current level
		if matchesIgnorePattern(key, ignoreList) {
			// Preserve this field from API
			result[key] = apiValue
			if debug {
				log.Printf("api_object.go: Preserving ignored field '%s' from API state", key)
			}
			continue
		}

		// If this key exists in both desired and API, and both are maps, recurse
		if desiredValue, exists := result[key]; exists {
			desiredMap, desiredIsMap := desiredValue.(map[string]interface{})
			apiMap, apiIsMap := apiValue.(map[string]interface{})

			if desiredIsMap && apiIsMap {
				// Descend the ignore list for this key
				descendedIgnoreList := _descendIgnoreList(key, ignoreList)

				// Recursively merge ignored fields in nested maps
				result[key] = mergeIgnoredFields(desiredMap, apiMap, descendedIgnoreList, debug)
			}
		}
	}

	return result
}

func (obj *APIObject) patchMidpointObject() error {
	// Write entry log
	debugFile := "/tmp/midpoint-patch-debug.log"
	f, _ := os.OpenFile(debugFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("\n===== ENTERING patchMidpointObject %s =====\n", time.Now().Format("2006-01-02 15:04:05")))
		f.WriteString(fmt.Sprintf("Object ID: %s\n", obj.id))
		f.WriteString(fmt.Sprintf("Data keys: %d\n", len(obj.data)))
		f.WriteString(fmt.Sprintf("API Data keys: %d\n", len(obj.apiData)))
		f.Close()
	}

	if obj.debug {
		log.Printf("api_object.go: Calculating differences for PATCH operation")
	}

	// For Midpoint REST API, data often has a wrapper key (e.g., {"role": {...}})
	// When PATCH-ing to /roles/{id}, we need to patch the fields inside the role, not the wrapper itself
	// So if there's a single top-level key, unwrap it
	workingData := obj.data
	workingApiData := obj.apiData

	if len(obj.data) == 1 && len(obj.apiData) == 1 {
		// Get the single key from both maps
		var dataKey, apiKey string
		for k := range obj.data {
			dataKey = k
		}
		for k := range obj.apiData {
			apiKey = k
		}

		// If both have the same single key and it's a map, unwrap it
		if dataKey == apiKey {
			if dataMap, ok := obj.data[dataKey].(map[string]interface{}); ok {
				if apiMap, ok := obj.apiData[apiKey].(map[string]interface{}); ok {
					workingData = dataMap
					workingApiData = apiMap
					if obj.debug {
						log.Printf("api_object.go: Unwrapped data from '%s' key for patching", dataKey)
					}
				}
			}
		}
	}

	// Prepare desired data by recursively preserving ignored fields from API state
	// This prevents ignored fields from being deleted, even when nested
	desiredData := make(map[string]interface{})
	for k, v := range workingData {
		desiredData[k] = v
	}

	// Recursively merge ignored fields from API data into desired data
	if len(obj.ignoreChangesTo) > 0 {
		desiredData = mergeIgnoredFields(desiredData, workingApiData, obj.ignoreChangesTo, obj.debug)
	}

	// Process each top-level key in the desired state
	for key, desiredValue := range desiredData {

		currentValue, exists := workingApiData[key]

		// Handle additions and modifications
		if !exists {
			// Key doesn't exist in current state - add it
			log.Printf("api_object.go: *** PATCH OPERATION: Adding new attribute '%s'", key)

			err := obj.sendMidpointPatch("add", key, desiredValue)
			if err != nil {
				return fmt.Errorf("failed to add attribute '%s': %v", key, err)
			}
		} else if !reflect.DeepEqual(currentValue, desiredValue) {
			// Key exists but value is different - replace it
			log.Printf("api_object.go: *** PATCH OPERATION: Replacing attribute '%s'", key)

			err := obj.sendMidpointPatch("replace", key, desiredValue)
			if err != nil {
				return fmt.Errorf("failed to replace attribute '%s': %v", key, err)
			}
		}
	}

	// Check for deletions - keys that exist in current state but not in desired state
	for key := range workingApiData {
		if _, exists := desiredData[key]; !exists {
			// Skip the ID attribute - we don't want to delete that
			if key == obj.idAttribute {
				continue
			}

			// Skip fields in the ignore list - these are server-managed and shouldn't be deleted
			if matchesIgnorePattern(key, obj.ignoreChangesTo) {
				if obj.debug {
					log.Printf("api_object.go: Skipping deletion of ignored attribute '%s'", key)
				}
				continue
			}

			if obj.debug {
				log.Printf("api_object.go: Deleting attribute '%s'", key)
			}

			err := obj.sendMidpointPatch("delete", key, nil)
			if err != nil {
				return fmt.Errorf("failed to delete attribute '%s': %v", key, err)
			}
		}
	}

	// After sending patches, Terraform will call Read() to refresh the state
	// So we don't need to explicitly read here
	return nil
}

// sendMidpointPatch sends a single PATCH request for the specified modification
func (obj *APIObject) sendMidpointPatch(modificationType string, path string, value interface{}) error {
	// Build the ObjectModificationType payload
	// Midpoint expects: { "objectModification": { "itemDelta": { "modificationType": "...", "path": "...", "value": ... } } }

	itemDelta := make(map[string]interface{})
	itemDelta["modificationType"] = modificationType
	itemDelta["path"] = path

	// Add value for add and replace operations
	if modificationType != "delete" && value != nil {
		// Filter out ignored fields from the value before sending
		// This prevents sending server-managed fields like @metadata, @ns, etc.
		if mapValue, ok := value.(map[string]interface{}); ok {
			value = filterIgnoredFields(mapValue, obj.ignoreChangesTo)
		} else if sliceValue, ok := value.([]interface{}); ok {
			// Handle arrays by filtering each element
			filteredSlice := make([]interface{}, len(sliceValue))
			for i, elem := range sliceValue {
				if mapElem, ok := elem.(map[string]interface{}); ok {
					filteredSlice[i] = filterIgnoredFields(mapElem, obj.ignoreChangesTo)
				} else {
					filteredSlice[i] = elem
				}
			}
			value = filteredSlice
		}
		itemDelta["value"] = value
	}

	// Wrap in objectModification as required by Midpoint's ObjectModificationType
	modification := map[string]interface{}{
		"objectModification": map[string]interface{}{
			"itemDelta": itemDelta,
		},
	}

	// Convert to JSON
	modificationJSON, err := json.Marshal(modification)
	if err != nil {
		return fmt.Errorf("failed to marshal modification to JSON: %v", err)
	}

	// Construct the PATCH path
	// NOTE: We don't include query_string for PATCH operations because options like
	// "isImport", "overwrite", "noFetch" are for create/import operations and cause
	// Midpoint to expect a full object (e.g., RoleType) instead of ObjectModificationType
	patchPath := obj.putPath
	fullPath := strings.Replace(patchPath, "{id}", obj.id, -1)

	// Write debug info to file for inspection
	debugFile := "/tmp/midpoint-patch-debug.log"
	f, _ := os.OpenFile(debugFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("\n===== PATCH REQUEST %s =====\n", time.Now().Format("2006-01-02 15:04:05")))
		f.WriteString(fmt.Sprintf("Method: PATCH\n"))
		f.WriteString(fmt.Sprintf("Full URL: %s%s\n", obj.apiClient.uri, fullPath))
		f.WriteString(fmt.Sprintf("Payload: %s\n", string(modificationJSON)))
		f.WriteString("================================\n")
		f.Close()
	}

	if obj.debug {
		log.Printf("api_object.go: ===== PATCH REQUEST DEBUG =====")
		log.Printf("api_object.go: Method: PATCH")
		log.Printf("api_object.go: Full URL: %s%s", obj.apiClient.uri, fullPath)
		log.Printf("api_object.go: Payload: %s", string(modificationJSON))
		log.Printf("api_object.go: ================================")
	}

	// Send the PATCH request
	resultString, err := obj.apiClient.sendRequest("PATCH", fullPath, string(modificationJSON))
	if err != nil {
		return err
	}

	// Update internal state if the API returns the updated object
	if obj.apiClient.writeReturnsObject {
		if obj.debug {
			log.Printf("api_object.go: Parsing response from PATCH to update internal structures (write_returns_object=true)...\n")
		}
		return obj.updateState(resultString)
	}

	return nil
}

func (obj *APIObject) findObject(queryString string, searchKey string, searchValue string, resultsKey string, searchData string) (map[string]interface{}, error) {
	var objFound map[string]interface{}
	var dataArray []interface{}
	var ok bool

	/*
	   Issue a GET to the base path and expect results to come back
	*/
	searchPath := obj.searchPath
	if queryString != "" {
		if obj.debug {
			log.Printf("api_object.go: Adding query string '%s'", queryString)
		}
		searchPath = fmt.Sprintf("%s?%s", obj.searchPath, queryString)
	}

	if obj.debug {
		log.Printf("api_object.go: Calling API on path '%s'", searchPath)
	}
	resultString, err := obj.apiClient.sendRequest(obj.apiClient.readMethod, searchPath, searchData)
	if err != nil {
		return objFound, err
	}

	/*
	   Parse it seeking JSON data
	*/
	if obj.debug {
		log.Printf("api_object.go: Response received... parsing")
	}
	var result interface{}
	err = json.Unmarshal([]byte(resultString), &result)
	if err != nil {
		return objFound, err
	}

	if resultsKey != "" {
		var tmp interface{}

		if obj.debug {
			log.Printf("api_object.go: Locating '%s' in the results", resultsKey)
		}

		/* First verify the data we got back is a hash */
		if _, ok = result.(map[string]interface{}); !ok {
			return objFound, fmt.Errorf("api_object.go: The results of a GET to '%s' did not return a hash. Cannot search within for results_key '%s'", searchPath, resultsKey)
		}

		tmp, err = GetObjectAtKey(result.(map[string]interface{}), resultsKey, obj.debug)
		if err != nil {
			return objFound, fmt.Errorf("api_object.go: Error finding results_key: %s", err)
		}
		if dataArray, ok = tmp.([]interface{}); !ok {
			return objFound, fmt.Errorf("api_object.go: The data at results_key location '%s' is not an array. It is a '%s'", resultsKey, reflect.TypeOf(tmp))
		}
	} else {
		if obj.debug {
			log.Printf("api_object.go: results_key is not set - coaxing data to array of interfaces")
		}
		if dataArray, ok = result.([]interface{}); !ok {
			return objFound, fmt.Errorf("api_object.go: The results of a GET to '%s' did not return an array. It is a '%s'. Perhaps you meant to add a results_key?", searchPath, reflect.TypeOf(result))
		}
	}

	/* Loop through all of the results seeking the specific record */
	for _, item := range dataArray {
		var hash map[string]interface{}

		if hash, ok = item.(map[string]interface{}); !ok {
			return objFound, fmt.Errorf("api_object.go: The elements being searched for data are not a map of key value pairs")
		}

		if obj.debug {
			log.Printf("api_object.go: Examining %v", hash)
			log.Printf("api_object.go:   Comparing '%s' to the value in '%s'", searchValue, searchKey)
		}

		tmp, err := GetStringAtKey(hash, searchKey, obj.debug)
		if err != nil {
			return objFound, fmt.Errorf("failed to get the value of '%s' in the results array at '%s': %s", searchKey, resultsKey, err)
		}

		/* We found our record */
		if tmp == searchValue {
			objFound = hash
			obj.id, err = GetStringAtKey(hash, obj.idAttribute, obj.debug)
			if err != nil {
				return objFound, fmt.Errorf("failed to find id_attribute '%s' in the record: %s", obj.idAttribute, err)
			}

			if obj.debug {
				log.Printf("api_object.go: Found ID '%s'", obj.id)
			}

			/* But there is no id attribute??? */
			if obj.id == "" {
				return objFound, fmt.Errorf("the object for '%s'='%s' did not have the id attribute '%s', or the value was empty", searchKey, searchValue, obj.idAttribute)
			}
			break
		}
	}

	if obj.id == "" {
		return objFound, fmt.Errorf("failed to find an object with the '%s' key = '%s' at %s", searchKey, searchValue, searchPath)
	}

	return objFound, nil
}
