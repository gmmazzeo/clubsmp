/*
 * Copyright 2015 ScAi, CSD, UCLA.
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
package edu.ucla.cs.scai.clubsp.messages;

/**
 *
 * @author Giuseppe M. Mazzeo <gmmazzeo@gmail.com>
 */
public class DummyMessage extends ClubsPMessage {

    String message;
    long idMain;

    public DummyMessage(String message, long idMain) {
        super();
        this.message = message;
        this.idMain = idMain;
    }

    public DummyMessage(long idMain) {
        super();
        this.message = "This is a default message. Enjoy!";
        this.idMain = idMain;
    }

    @Override
    public String toString() {
        return "DummyMessage{" + "message=" + message + ", idMain=" + idMain + '}';
    }

}
