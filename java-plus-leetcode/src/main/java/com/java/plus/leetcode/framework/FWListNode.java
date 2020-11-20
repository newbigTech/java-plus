package com.java.plus.leetcode.framework;

import com.java.plus.leetcode.model.ListNode;
import com.java.plus.leetcode.util.ListNodeUtils;

/**
 * 链表遍历框架，兼具迭代和递归结构
 */
public class FWListNode {
	
	public static void main(String[] args) {
		ListNode head=ListNodeUtils.createListNode();
		ListNodeUtils.printListNode(head);
		ListNode newhead=reverse(head); 
		ListNodeUtils.printListNode(newhead);
		System.out.println("------------------------"); 
		head=ListNodeUtils.createListNode();
		ListNodeUtils.printListNode(head);
		ListNode newnode=iteration(head); 
		ListNodeUtils.printListNode(newnode);

		System.out.println("------------------------"); 
		head=ListNodeUtils.createListNode();
		ListNodeUtils.printListNode(head);
		ListNode newnode1=reverse1(head); 
		ListNodeUtils.printListNode(newnode1);
	}

	/**
	 * 递归反转整个链表
	 * 递归算法:输入一个节点 head，将「以 head 为起点」的链表反转，并返回反转之后的头结点。
	 * */
	public static ListNode reverse(ListNode head) {
		if (head.next == null)
			return head;
		ListNode last = reverse(head.next);
		head.next.next = head; 
		head.next = null;//避免1->2和2->1
		return last;
	}
	
	// 反转以 a 为头结点的链表
	public static ListNode reverse1(ListNode a) {
	    ListNode pre, cur, nxt;
	    pre = null; cur = a; nxt = a;
	    while (cur != null) {
	        nxt = cur.next;
	        // 逐个结点反转
	        cur.next = pre;
	        // 更新指针位置
	        pre = cur;
	        cur = nxt;
	    }
	    // 返回反转后的头结点
	    return pre;
	}
	
	public static ListNode iteration(ListNode head) { 
		ListNode node = null;
		ListNode temp = null;
		while(head!=null) {
			temp=head.next;
			head.next=node;
			node=head;
			head=temp;  
		}
		return node;
	}

	public void traverse(ListNode head) {
		for (ListNode p = head; p != null; p = p.next) {
			// 迭代访问 p.val
		}
	}

//	public void traverse(ListNode head) {
//	    // 递归访问 head.val
//	    traverse(head.next);
//	} 

}
