/*
 * Copyright 2016 EMBL-EBI.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package utils;

/**
 *
 * @author asenf
 */
import java.util.TimerTask;
import java.util.concurrent.Future;

/**
 *
 * @author asenf
 * 
 * Perform Periodic Maintenance tasks - TODO
 * 
 */
public class MyTimerTask extends TimerTask {
    
    private static Future[] f;
    
    public MyTimerTask(Future[] f) {
        MyTimerTask.f = f;
    }
    
    public void setF(Future[] f) {
        MyTimerTask.f = f;
    }
    
    @Override
    public void run() {
        StringBuilder sb = new StringBuilder();

        int j = 0;
        if (MyTimerTask.f != null) {
            for (int i=0; i<MyTimerTask.f.length; i++) {
                if (MyTimerTask.f[i]!=null && MyTimerTask.f[i].isDone())
                    j++;
            }
        }

        sb.append("Download Active: ").append(MyTimerTask.f.length)
                .append(" jobs submitted, ").append(j).append(" completed.").append("\n");
        
        System.out.println(sb.toString());
    }
    
}
