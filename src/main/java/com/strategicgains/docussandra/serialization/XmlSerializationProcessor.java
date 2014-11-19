package com.strategicgains.docussandra.serialization;

import org.restexpress.serialization.xml.XstreamXmlProcessor;

import com.strategicgains.docussandra.domain.Namespace;

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
