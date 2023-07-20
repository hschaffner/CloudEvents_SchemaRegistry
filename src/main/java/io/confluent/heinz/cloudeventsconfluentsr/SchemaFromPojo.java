/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.heinz.cloudeventsconfluentsr;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaFromPojo {
    public static String getJsonSchema(Class clazz) throws IOException {
        Field[] fields = clazz.getDeclaredFields();
        List<Map<String,String>> map=new ArrayList<Map<String,String>>();
        for (Field field : fields) {
            HashMap<String, String> objMap=new HashMap<String, String>();
            objMap.put("name", field.getName());
            objMap.put("type", field.getType().getSimpleName());
            objMap.put("format", "");
            map.add(objMap);
        }
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(map);

        return json;
    }
}
