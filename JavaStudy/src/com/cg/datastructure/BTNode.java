
package com.cg.datastructure;

import java.util.*;

public class BTNode {

    public BTNode(char data, BTNode leftNode, BTNode rightNode) {
        this.data = data;
        this.leftNode = leftNode;
        this.rightNode = rightNode;
    }

    public char data;
    public BTNode leftNode;
    public BTNode rightNode;

    public void preOrder() {
        if (leftNode != null)
            leftNode.preOrder();
        if (rightNode != null)
            rightNode.preOrder();
        System.out.println(data);

    }

    public static void main(String[] args) {
        //		Calendar calendar = Calendar.getInstance();
        //		System.out.println(calendar.getClass());
        //		calendar.set(Calendar.DATE, calendar.getMinimum(Calendar.DAY_OF_MONTH));
        //		calendar.set(Calendar.HOUR_OF_DAY, 0);
        //		calendar.set(Calendar.MINUTE, 0);
        //		calendar.set(Calendar.SECOND, 0);
        //		calendar.set(Calendar.MILLISECOND, 0);
        //
        //		Date endTime = calendar.getTime();
        //		calendar.add(Calendar.MONTH, -1);
        //
        //		Date beginTime = calendar.getTime();
        //
        //		System.out.println(endTime);
        //		System.out.println(beginTime);

        //把当前时间调整到当天0点0分0秒
        //		Calendar date = Calendar.getInstance();
        //		date.set(Calendar.HOUR_OF_DAY, 24);
        //		date.set(Calendar.MINUTE, 0);
        //		date.set(Calendar.SECOND, 0);
        //
        //		Date endDay = date.getTime();
        //		System.out.println(endDay);
        //
        //
        //		//将时间设置为[dayNum-1]天前的0点0分0秒
        //		date.add(Calendar.DAY_OF_MONTH, -3);
        //		Date queryDay = date.getTime();
        //		System.out.println(queryDay);
        //		Boolean b = null;
        //		if (b==null||b){
        //			System.out.println(b);
        //		}

        //		BTNode nodeO = new BTNode( 'O', null, null );
        //		BTNode nodeR = new BTNode( 'R', null, null );
        //		BTNode nodeT = new BTNode( 'T', null, null );
        //
        //		BTNode nodeL = new BTNode( 'L', nodeO, nodeR );
        //		BTNode nodeG = new BTNode( 'G', null, nodeT );
        //		BTNode nodeA = new BTNode( 'A', nodeL, nodeG );
        //
        //		nodeA.preOrder( );

        //		int[] array = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        //		System.out.println( "3在数组中的位置:" + search( array, 3 ) );

        //		int a =2;
        //		int b =6;
        //		double r = a/b;
        //		System.out.println(r);

    }

    private static int search(int[] array, int target) {
        return binarySearch(0, array.length - 1, array, target);
    }

    private static int binarySearch(int start, int end, int[] array,
            int target) {
        if (end < start || start < 0) {
            return -1;
        }
        int middle = (start + end) / 2;
        if (target == array[middle]) {
            return middle;
        } else if (target < array[middle]) {
            end = middle - 1;
        } else {
            start = middle + 1;
        }
        return binarySearch(start, end, array, target);
    }

    private static void testLoop(int i) {
        while (i < 1) {
            System.out.println(i);
            testLoop(i++);
        }
    }

    public static void getSystemProp() {

        // Map map = System.getenv( );
        // Iterator it = map.entrySet( ).iterator( );
        // while ( it.hasNext( ) )
        // {
        // Entry entry = (Entry) it.next( );
        // System.out.print( entry.getKey( ) + "=" );
        // System.out.println( entry.getValue( ) );
        // }
        System.out.println("current system environment variable:");
        Map<String, String> map = System.getenv();
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        System.out.println("---------------");
        System.out.println("current system properties:");
        Properties properties = System.getProperties();
        Set<Map.Entry<Object, Object>> set = properties.entrySet();
        for (Map.Entry<Object, Object> objectObjectEntry : set) {
            System.out.println(objectObjectEntry.getKey() + ":"
                    + objectObjectEntry.getValue());
        }

    }

    public static String[] localSplit(String str, int limit) {
        char ch = 'a';
        int off = 0;
        int next = 0;
        boolean limited = limit > 0;
        ArrayList<String> list = new ArrayList<>();
        while ((next = str.indexOf(ch, off)) != -1) {
            if (!limited || list.size() < limit - 1) {
                list.add(str.substring(off, next));
                off = next + 1;
            } else { // last one
                // assert (list.size() == limit - 1);
                list.add(str.substring(off, str.length()));
                off = str.length();
                break;
            }
        }
        // If no match was found, return this
        if (off == 0)
            return new String[] { str };

        // Add remaining segment
        if (!limited || list.size() < limit)
            list.add(str.substring(off, str.length()));

        // Construct result
        int resultSize = list.size();
        if (limit == 0)
            while (resultSize > 0 &&
                    list.get(resultSize - 1).length() == 0)
                resultSize--;
        String[] result = new String[resultSize];
        return list.subList(0, resultSize).toArray(result);
    }

}
