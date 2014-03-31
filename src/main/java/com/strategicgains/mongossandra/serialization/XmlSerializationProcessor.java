package com.strategicgains.mongossandra.serialization;

import org.restexpress.serialization.xml.XstreamXmlProcessor;

import com.strategicgains.mongossandra.domain.Namespace;

public class XmlSerializationProcessor
extends XstreamXmlProcessor
{
	public XmlSerializationProcessor()
    {
	    super();
	    alias("namespace", Namespace.class);
//		alias("element_name", Element.class);
//		alias("element_name", Element.class);
//		alias("element_name", Element.class);
//		alias("element_name", Element.class);
		registerConverter(new XstreamUuidConverter());
    }
}
