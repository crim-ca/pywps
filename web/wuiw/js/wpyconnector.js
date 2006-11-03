/**********************************************************************
 * 
 * purpose: init sequence for DHTML interface
 *
 * authors: Luca Casagrande (...) and Lorenzo Becchi (lorenzo@ominiverdi.com)
 * some code got from: Paul Spencer wms connector for ka-Map.
 * TODO:
 *   - a lot...
 * 
 **********************************************************************
 *
 * Copyright (C) 2006 ominiverdi.org
 *  
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 **********************************************************************/
function wpsConnector(console) {
	initDHTMLAPI();
    this.server;
	this.ajax = new Ajax;
    this.baseURL;
    this.aServer = new Array();
    this.console = console;
    this.formObj;//obj to draw form fields
    this.version = '0.4.0';
    this.sessionId;

	this.XMLcode = '';//temporaneo per metterci su il GML della feature

 };
 
 wpsConnector.prototype.addServer = function (title, url){
	 this.aServer.push([title,url]);
 };
 
 
 wpsConnector.prototype.drawInitForm = function (objId){
	 
	this.formObj = getRawObject(objId);
	//set title
	var h2 = document.createElement('h2');
	h2.innerHTML = 'WPS connector';
	this.formObj.appendChild(h2);
	//set description
	var p = document.createElement('p');
	p.innerHTML = 'Use the select box here below to choose a server. Wait for remote response.';
	this.formObj.appendChild(p);
	
	var form = document.createElement('form');
	form.name='connector';
	form.action='#';
	
	//Make server select
	var select = document.createElement('select');
	select.name = 'serverlist';
	select.id = 'serverlist';
//	select.wpsConnector=this;
	select.onchange = this.connect2server;
	var j = 0;
	var opt = new Option( 'select a server', '', true, true );
	select[j++] = opt;
	for(i=0;i<this.aServer.length;i++) {
		select[j++] = new Option(this.aServer[i][0],this.aServer[i][1],false,false);
	}
	form.appendChild(select);
	var input = document.createElement('input');
	input.type='button';
	input.value='go';
	input.wpsConnector=this;
	input.onclick=this.connect2server;
	//form.appendChild(input);
	this.formObj.appendChild(form);
 };
 
 
 wpsConnector.prototype.connect2server = function (){
	 var self = wpsConnector;
	 var select = getRawObject('serverlist');
	 
	 var url = select[select.selectedIndex].value;
	 
	 if(url.length>0) {
		 this.baseURL = url;
	 } else {
		 alert('select a server first');
		 return;
	 } 
    if (this.baseURL.indexOf('?') == -1)
    {
        this.baseURL = this.baseURL + '?';
    }
    else
    {
        if (this.baseURL.charAt( this.baseURL.length - 1 ) == '&')
            this.baseURL = this.baseURL.slice( 0, -1 );
    }
	
    this.baseURL = this.baseURL+ "&service=wps";
    this.baseURL = this.baseURL + "&request=GetCapabilities";
    this.baseURL = this.baseURL +  "&version="+self.version;

    var connector = 'tools/wps/wps_connector.php?';
    var myURL = connector + 'wpsURL='+myurlencode(this.baseURL);
    self.ajax.doGet(myURL, self.parseCapabilities,'xml');
 
 };
 
 
 wpsConnector.prototype.parseCapabilities = function (xml){
	 //var tot = szText.getElemtentsByTagName('Process');
	 //alert(typeof xml);
	 var self = wpsConnector;
	 if(typeof xml=='object'){
		 //alert(xml);
		 var aProcess = xml.getElementsByTagName('Process');
		 if(aProcess.length>0){
			// alert(this.id);
			 self.drawProcessForm(xml);
		 } else {
				alert('no Processes availalbe on this server');
		 }
	 } else {
		alert('connection error: response is not an object');
	 }
 };
 
 
 wpsConnector.prototype.drawProcessForm = function(xml){
	 	var self= wpsConnector;
	 	var aProcess = xml.getElementsByTagName('Process');
		 
		var p = document.createElement('p');
		p.innerHTML = 'choose a Process';
		self.formObj.appendChild(p);
		var select = document.createElement('select');
		select.name = 'processlist';
		select.onchange = this.getProcess;
		select.id = 'processlist';
		var j = 0;
		var opt = new Option( 'select a Process', '', true, true );
		select[j++] = opt;
		for(i=0;i<aProcess.length;i++){
			var processes = aProcess[i].getElementsByTagName('Identifier');
			//alert(processes[0].textContent);//nodeValue,localName,tagName,textContent
			var name = processes[0].textContent;
			select[j++] = new Option(name,name,false,false);
		}
		self.formObj.appendChild(select);
		var input = document.createElement('input');
		input.type='button';
		input.value='go';
		input.processes=aProcess;
		input.onclick=this.getProcess;
		//self.formObj.appendChild(input);
 };
 
 
  wpsConnector.prototype.getProcess = function (){
	  var self = wpsConnector;
	  var select = getRawObject('processlist');
	  var process = trim(select[select.selectedIndex].value);
	  
	   var select2 = getRawObject('serverlist');
	 var url = select2[select2.selectedIndex].value;
	 
	 if(process.length>0) {
		 this.baseURL = url;
	 } else {
		 alert('select a process first');
		 return;
	 }
	
	 if (this.baseURL.indexOf('?') == -1)
    {
        this.baseURL = this.baseURL + '?';
    }
    else
    {
        if (this.baseURL.charAt( this.baseURL.length - 1 ) == '&')
            this.baseURL = this.baseURL.slice( 0, -1 );
    }
	this.baseURL = this.baseURL + "&service=wps&request=DescribeProcess&version="+ self.version;
    this.baseURL = this.baseURL +  "&identifier="+process ;
    var connector = 'tools/wps/wps_connector.php?';
    var myURL = connector + 'wpsURL='+myurlencode(this.baseURL);
    //myURL = this.wpsConnector.addRequestParameter(myURL, 'com', "&com=getCapabilities" );
    self.ajax.doGet(myURL, self.parseProcesses,'xml');
	 
  };
  
  
  wpsConnector.prototype.parseProcesses = function(xml){
	  var self = wpsConnector;
	 if(typeof xml=='object'){
		 //alert(xml);
		 var aProcess = xml.getElementsByTagName('ProcessDescription');
		 if(aProcess.length>0){
			// alert(this.id);
			 self.drawProcessDescription(xml);
		 } else {
				alert('no Processes availalbe on this server');
		 }
	 } else {
		alert('connection error: response is not an object');
	 }
	  
  };
  
   wpsConnector.prototype.drawProcessDescription = function (xml) {
	   var self = wpsConnector;
	   var description = getRawObject('description');
	   
	   var identifier = xml.getElementsByTagName('Identifier')[0].textContent;
	   var title = xml.getElementsByTagName('Title')[0].textContent;
	   var labstract = xml.getElementsByTagName('Abstract')[0].textContent;
	   
	   var h2 = document.createElement('h2');
	   h2.innerHTML = 'Identifier: ' + identifier;
	   var h3 = document.createElement('h3');
	   h3.innerHTML = 'Title: ' + title;
	   var p = document.createElement('p');
	   p.innerHTML = labstract;
	   description.appendChild(h2);
	   description.appendChild(h3);
	   description.appendChild(p);
	  
   };



wpsConnector.prototype.getFeatures = function(sessionId,features)
{
  /*???*/
  var featURL = this.server;
  featURL = this.addRequestParameter(featURL, 'service', "&service=WFS" );
  featURL =   this.addRequestParameter(featURL, 'request', "&request=GetFeature" );
  featURL =   this.addRequestParameter(featURL, 'version', "&version="+this.version );
  featURL =   this.addRequestParameter(featURL, 'typename', "&typename="+features );
 
  myURL = this.connector + 'wfsURL='+encodeMyHtml(featURL);
  myURL =   this.addRequestParameter(myURL, 'sessionId', "&sessionId="+this.sessionId );
  myURL =   this.addRequestParameter(myURL, 'com', "&com=getFeature" );
  myURL =   this.addRequestParameter(myURL, 'epsg', "&epsg="+this.epsg );
   //document.getElementById('legend').innerHTML = myURL;//DEBUG
   
  call(myURL,this, this.draw);
  
};

