/*
    Copyright 2015, Strategic Gains, Inc.

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
package com.strategicgains.docussandra.domain;

import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author toddf
 * @since Jan 25, 2015
 */
public class Credentials
{
	@Required("Namespace")
	private DatabaseReference namespace;

	@Required("Username")
	private String username;

	@Required("Password")
	private String password;
}
