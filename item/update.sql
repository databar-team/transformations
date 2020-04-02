--Update fields in edw_items (MF01) from edw_item_descriptors (MF02).

    UPDATE /*+ RULE */ edw_items i
       SET cover_size =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'COVER SIZE'),
	   dimensions =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'DIMENSIONS'),
	   item_size =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'SIZE'),
	   style =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'STYLE'),
	   type =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'TYPE'),
	   material =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'MATERIAL'),
	   color =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'COLOR'),
	   degree =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'DEGREE'),
	   lot_size =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'LOT SIZE'),
	   metal_quality =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'METAL QUALITY'),
	   apparel_color =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'APPAREL COLOR'),
	   apparel_size =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'APPAREL SIZE'),
	   quality =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'QUALITY'),
	   length =
	     (SELECT segment_value
		FROM edw_item_descriptors d
	       WHERE d.inventory_item_id = i.inventory_item_id
		 AND d.segment_name = 'LENGTH'),
		Design = 
		(SELECT segment_value
				  FROM edw_item_descriptors d
				 WHERE d.inventory_item_id = i.inventory_item_id
					   AND d.segment_name = 'DESIGN') ,  -- CR# 31317
		trim_size = 
		(SELECT segment_value
				  FROM edw_item_descriptors d
				 WHERE d.inventory_item_id = i.inventory_item_id
					   AND d.segment_name = 'TRIM SIZE') ,  -- CR# 31317
	   system_update_date_time = SYSDATE
    WHERE inventory_item_id IN (
           -- If descriptors are created/updated, then for all related items
           --   the item descriptors must be updated
           SELECT inventory_item_id
             FROM edw_item_descriptors u
            WHERE system_update_date_time >= g_begin_date
           UNION
           -- If items are created/updated after descriptors are 
           --  created/updated, the item descriptors must be updated
           SELECT inventory_item_id
             FROM edw_items
            WHERE system_update_date_time >= g_begin_date);

 
