/*
   Copyright 2007 Fourspaces Consulting, LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.fourspaces.couchdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONArray;

/**
 * The View is the mechanism for performing Querys on a CouchDB instance. The
 * view can be named or ad-hoc (see AdHocView). (Currently [14 Sept 2007] named
 * view aren't working in the mainline CouchDB code... but this _should_ work.)
 *<p>
 * The View object exists mainly to apply filtering to the view. Otherwise,
 * views can be called directly from the database object by using their names
 * (or given an ad-hoc query).
 * 
 * @author mbreese
 * 
 */
public class View {
	protected String name;
	protected Document document;
	protected String function;
	protected Map<String,String> queryParams = new HashMap<String,String>();

	/**
	 * Build a view given a document and a name
	 * 
	 * @param doc
	 * @param name
	 */
	public View(Document doc, String name) {
		this.document = doc;
		this.name = name;
	}

	/**
	 * Build a view given only a fullname ex: ("_add_docs", "_temp_view")
	 * 
	 * @param fullname
	 */
	public View(String fullname) {
		this.name = fullname;
		this.document = null;
	}

	/**
	 * Builds a new view for a document, a given name, and the function
	 * definition. This <i>does not actually add it to the document</i>. That is
	 * handled by Document.addView()
	 * <p>
	 * This constructor should only be called by Document.addView();
	 * 
	 * @param doc
	 * @param name
	 * @param function
	 */
	View(Document doc, String name, String function) {
		this.name = name;
		this.document = doc;
		this.function = function;
	}

	/**
	 * Based upon settings, builds the queryString to add to the URL for this
	 * view.
	 * 
	 * 
	 * @return
	 */
	public String getQueryString() {
		StringBuffer queryStringBuffer = new StringBuffer();
		
		for(Entry<String, String> param : queryParams.entrySet()) {
			if(null != param.getValue() && !"".equals(param.getValue())) {
				if(queryStringBuffer.length() > 0) {
					queryStringBuffer.append("&");
				}
				queryStringBuffer.append(param.getKey());
				queryStringBuffer.append("=");
				queryStringBuffer.append(param.getValue());
			}
		}
		
		String queryString = queryStringBuffer.toString();
		return queryString.equals("") ? null : queryString;

	}

	public void setKey(String key) {
		if(!key.startsWith("\"")) {
			key = "\"" + key + "\"";
		}
		queryParams.put("key", key);
	}

	/**
	 * Start listing at this key
	 * 
	 * @param startKey
	 */
	public void setSingleStartKey(String startKey) {
		if(null != queryParams) {
			setStartKey("[\""+startKey+"\"]");
		} else {
			setStartKey((String) null);
		}
	}

	public void setStartKey(JSONArray startKey) {
		setStartKey(startKey.toString());
	}
	
	public void setStartKey(String startKey) {
		queryParams.put("startkey", startKey);
	}

	/**
	 * Stop listing at this key
	 * 
	 * @param endKey
	 */
	public void setSingleEndKey(String endKey) {
		if(null != queryParams) {
			setEndKey("[\""+endKey+"\"]");
		} else {
			setEndKey((String) null);
		}
	}

	public void setEndKey(JSONArray endKey) {
		setEndKey(endKey.toString());
	}
	
	public void setEndKey(String endKey) {
		queryParams.put("endkey", endKey);
	}

	/**
	 * The number of entries to return
	 * 
	 * @param count
	 * @deprecated CouchDB 0.9 uses limit instead
	 */
	public void setCount(Integer count) {
		// this.count = count;
		setLimit(count);
	}

	public void setLimit(Integer limit) {
		queryParams.put("limit", String.valueOf(limit));
	}

	/**
	 * Reverse the listing
	 * 
	 * @param reverse
	 * @deprecated CouchDB 0.9 uses "descending" instead
	 */
	public void setReverse(Boolean reverse) {
		setDescending(reverse);
	}

	public void setDescending(Boolean descending) {
		setBooleanParameter("descending", descending);
	}

	public void setGroup(Boolean group) {
		setBooleanParameter("group", group);
	}

	/**
	 * Skip listing these keys (not sure if this works, or the format)
	 * 
	 * @param skip
	 */
	public void setSkip(int skip) {
		queryParams.put("skip", String.valueOf(skip));
	}

	/**
	 * Not sure... might be for batch updates, but not sure.
	 * 
	 * @param update
	 */
	public void setUpdate(Boolean update) {
		setBooleanParameter("update", update);
	}

	/**
	 * The name for this view (w/o doc id)
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * the full name for this view (w/ doc id, if avail) in the form of :
	 * "docid:name" or "name"
	 * 
	 * @return
	 */
	public String getFullName() {
		return (document == null) ? name : document.getViewDocumentId() + "/"
				+ name;
	}

	/**
	 * The function definition for this view, if it is available.
	 * 
	 * @return
	 */
	public String getFunction() {
		return function;
	}
	
	private void setBooleanParameter(String param, Boolean value) {
		if(value) {
			queryParams.put(param, String.valueOf(value));
		} else {
			queryParams.remove(param);
		}
	}

}
