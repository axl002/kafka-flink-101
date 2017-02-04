package com.grallandco.demos;

import java.util.List;

public class RawFormat {
    public String next_change_id;
    public String stashes;

    public RawFormat() {
    }

    public void setNext_change_id(String id) {
        next_change_id = id;
    }

    public void setStashes(String allStashes) {
        stashes = allStashes;
    }

    public String getnext_change_id() {
        return next_change_id;
    }


    public static class AllStash {

        public String stashList;


        public void setStashList(String stashArray) {
            stashList = stashArray;
        }

        public String getStashList() {
            return stashList;
        }

    }

    public static class StashList {

        public ItemList items;
        public String accountName;
        public String lastCharacterName;
        public String id;
        public String stash;
        public String stashType;

        public StashList() {
        }

        public String getAccountName() {
            return accountName;
        }

        public void setAccountName(String accountName) {
            this.accountName = accountName;
        }


        public String getLastCharacterName() {
            return lastCharacterName;
        }

        public void setLastCharacterName(String lastCharacterName) {
            this.lastCharacterName = lastCharacterName;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getStash() {
            return stash;
        }

        public void setStash(String stash) {
            this.stash = stash;
        }

        public String getStashType() {
            return stashType;
        }


        public ItemList getItems() {
            return items;
        }

        public void setItems(ItemList items) {
            this.items = items;
        }

    }

    public static class ItemList {
        public Item item;

        public ItemList() {
        }

        public Item getItem() {
            return item;
        }

        public void setItem(Item item) {
            this.item = item;
        }

    }

    public static class Item{

        public String x;
        public String y;
        public String ilvl;
        public String icon;
        public String League;
        public String itemID;
        public String name;
        public String typeLine;
        public String note;
        public boolean identified;
        public boolean verified;
        public PropertyList propertyList;
        public List<String> explicitMods;
        public String flavorText;
        public String inventoryID;

        public String getTypeLine(){
            return this.typeLine;
        }

        public void setTypeLine(String typeLine){
            this.typeLine = typeLine;
        }

    }

public static class PropertyList {
    public ItemProperty itemProperty;



}

public static class ItemProperty {
    public String name;
    public int type;
}

}