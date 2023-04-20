exports.create = async (data, res, user) => {
    console.log('ENTRO A SERVICES');
    let validate = ajv.compile(schemaCreate)
    let valid = validate(data)

    if (!valid) {
      localize[res.getLocale()](validate.errors)
      return {
        code: 400,
        err: ajv.errorsText(validate.errors, { separator: ', ' }),
      }
    }
    if ('picking' in data && data.picking) {
      data.flag.picking = data.picking
    }
    delete data.picking
    let commit
    let brain = true
    let onlyBrain = false

    let { client, pool, session } = await Connection()

    let commerce = false,
      findByCode = false,
      queryPoint = [],
      paramsFind = {}
    if (data.pointSale === '-') {
      findByCode = true
      queryPoint.push(
        `first((SELECT @rid as rid, brain, onlyBrain FROM PointSale WHERE code = :pointSale)) as pointSale`
      )
      paramsFind.pointSale = data.pointSaleCode
    }
    if (data.paymentMethod.rid === '-') {
      if (onlinePayments.includes(data.paymentMethod.code)) {
        data.paymentMethod.code = 'online'
      }
      findByCode = true
      queryPoint.push(
        `first((SELECT @rid as rid,method FROM PaymentMethod WHERE code = :paymentCode)) as paymentMethod`
      )
      paramsFind.paymentCode = data.paymentMethod.code
    } else if (data.paymentMethod.rid !== '-') {
      findByCode = true
      queryPoint.push(
        `first((SELECT @rid as rid,method FROM PaymentMethod WHERE @rid = :paymentCode)) as paymentMethod`
      )
      paramsFind.paymentCode = data.paymentMethod.rid
    }
    if (data.city.rid === '-') {
      findByCode = true
      queryPoint.push(
        `first((SELECT @rid as rid FROM City WHERE code = :cityCode)) as city`
      )
      paramsFind.cityCode = data.city.code
    }
    if (findByCode) {
      // console.log(queryPoint.toString(),'El select por codido');
      // console.log(paramsFind,'El select param');
      let queryFindByRid = await session
        .query(`SELECT ${queryPoint.toString()}`, {
          params: paramsFind,
        })
        .one()

      if (data.pointSale === '-') {
        data.pointSale =
          queryFindByRid.pointSale && queryFindByRid.pointSale.rid
            ? queryFindByRid.pointSale.rid.toString()
            : ''
        brain = queryFindByRid.pointSale.brain
        onlyBrain = queryFindByRid.pointSale.onlyBrain
      }
      if (data.paymentMethod.rid === '-') {
        if (
          !queryFindByRid.paymentMethod ||
          (queryFindByRid.paymentMethod && !queryFindByRid.paymentMethod.rid)
        ) {
          session.close()
          pool.close()
          client.close()
          return { code: 400, err: res.__('paymentMethod_notfound') }
        }
        data.paymentMethod.rid = queryFindByRid.paymentMethod.rid.toString()
        data.paymentMethod.method = queryFindByRid.paymentMethod.method
      } else if (data.paymentMethod.rid !== '-') {
        if (
          !queryFindByRid.paymentMethod ||
          (queryFindByRid.paymentMethod && !queryFindByRid.paymentMethod.rid)
        ) {
          session.close()
          pool.close()
          client.close()
          return { code: 400, err: res.__('paymentMethod_notfound') }
        }
        data.paymentMethod.rid = queryFindByRid.paymentMethod.rid.toString()
        data.paymentMethod.method = queryFindByRid.paymentMethod.method
      }
      if (data.city.rid === '-') {
        if (
          !queryFindByRid.city ||
          (queryFindByRid.city && !queryFindByRid.city.rid)
        ) {
          session.close()
          pool.close()
          client.close()
          return { code: 400, err: res.__('city_notfound') }
        }
        data.city.rid = queryFindByRid.city.rid.toString()
      }
    }
    if (data.paymentMethod.rid === '#92:1') {
      data.paymentMethod.code = 'cash'
    }
    if (
      data.destinationLocation.lat === 0 &&
      data.destinationLocation.lng === 0
    ) {
      let direc = { city: data.city.name, direccion: data.address }
      try {
        let direction = await getCoordByDir(direc)
        data.destinationLocation.lat = parseFloat(direction.direction.latitude)
        data.destinationLocation.lng = parseFloat(direction.direction.longitude)
        //console.log(data.destinationLocation,'Entra acá')
        if (
          data.destinationLocation.lat === 0 &&
          data.destinationLocation.lng === 0
        ) {
          data.destinationLocation.lat = 4.1995619
          data.destinationLocation.lng = -74.634627
        }
      } catch (error) {
        data.destinationLocation.lat = 4.1995619
        data.destinationLocation.lng = -74.634627
      }
      //return res.json('aca');
    }

    try {
      commerce = await getPointSale(data.pointSale, session)
      // console.log(commerce,'Este comercio')
      brain = commerce.pointSale.brain
      onlyBrain = commerce.pointSale.onlyBrain
    } catch (error) {
      commerce = false
      brain = false
      onlyBrain = false
    }

    let coverage = { zone: false, rid: '' }
    // console.log();
    // return res.json(true);
    try {
      // coverage = await converResult(data.destinationLocation);
      coverage = await converResultByPointSale(
        data.destinationLocation,
        commerce.pointSale.zones
      )
    } catch (error) {
      // console.log(error, "este error");
      coverage = { zone: false }
    }

    // return commerce

    if (!commerce) {
      pool.close()
      session.close()
      client.close()
      return { code: 400, err: res.__('pointsale_notfound') }
    }

    let zones = []
    if (commerce.pointSale.zones) {
      for (let i = 0; i < commerce.pointSale.zones.length; i++) {
        if (commerce.pointSale.zones[i]) {
          zones.push(commerce.pointSale.zones[i].toString())
        }
      }
    }

    let queryDataphone = ``
    // console.log(coverage, "Coverage");

    // return res.json(true);

    if (data.dataPhone && data.dataPhone.rid) {
      data.dataPhone.isSpdy = 'false'
      queryDataphone = `, first((select spdy from ${data.dataPhone.rid})).spdy as isSpdy`
    }
    let queryState = `select @rid as rid, code, name, options ${queryDataphone} from ServiceState where code = "request"`
    let state = await session.query(queryState).one()
    if (!coverage.zone) {
      queryState = `select @rid as rid, code, name, options ${queryDataphone} from ServiceState where code = "out-of-range"`
      state = await session.query(queryState).one()
    }

    if (!state) {
      pool.close()
      session.close()
      client.close()
      return { code: 400, err: res.__('state_notfound') }
    } else {
      data.state = state
    }
    // console.log(`select @rid as rid, code, name, options ${queryDataphone} from ServiceState where code = "request"`)
    // return true;

    //Se agrega costo de servicio desde la creación
    //console.log(data.distanceMatrix, "Distancia");
    if (data.costDelivery === 0) {
      let quoteData = {
        pointSale: data.pointSale,
        destination: data.destinationLocation,
        returned: data.returned,
        multiDestiny: data.multiDestiny,
        totalPrice: data.subtotal,
        flag: data.flag,
      }
      try {
        let quoteDataR = await this.quote(quoteData, res, null)
        data.distanceMatrix = quoteDataR.distanceMatrix
        data.costDelivery = quoteDataR.costDelivery
        data.detailsCommerce = quoteDataR.detailsCommerce
      } catch (error) {
        console.log(error, 'error quote')
      }

      //console.log(quoteDataR)
    }
    if (
      data.distanceMatrix != undefined &&
      'distance' in data.distanceMatrix &&
      data.distanceMatrix.distance !== 0
    ) {
      data.distanceTime = {
        distance: parseFloat(data.distanceMatrix.distance.value / 1000).toFixed(
          1
        ),
        time: parseFloat(data.distanceMatrix.duration.value / 60).toFixed(0),
      }
      data.deliveryDistance = data.distanceTime.distance
      data.deliveryCost = data.costDelivery
      data.details = data.detailsCommerce
      delete data.detailsCommerce
    }

    //Se finaliza la adicion de costo desde la creación
    let multistop = [
      {
        latitude: data.destinationLocation.lat,
        longitude: data.destinationLocation.lng,
      },
    ]
    if ('multistop' in data.flag && data.flag.multistop) {
      // console.log(data.multiDestiny,'entra aca')
      multistop = []
      let mulD = data.multiDestiny.sort((a, b) =>
        a.numberstop > b.numberstop ? 1 : -1
      )
      mulD.map((value, index) => {
        let coordinatesArray = value.destiny.split(',')
        multistop.push({
          latitude: coordinatesArray[0],
          longitude: coordinatesArray[1],
        })
      })
    }
    // console.log(multistop, "MultiStop");
    // return true;
    let sendPost = {
      token: wsMatchDeliveryMan.token,
      company: commerce.name,
      storeName: commerce.pointSale.name,
      storeId: commerce.pointSale.rid.toString(),
      coordinates: multistop,
      sourceLatitude: commerce.pointSale.location.lat,
      sourceLongitude: commerce.pointSale.location.lng,
      destinationLatitude: data.destinationLocation.lat,
      destinationLongitude: data.destinationLocation.lng,
      description: data.description ? data.description : '',
      treshold: commerce.pointSale.enlistmentTime
        ? commerce.pointSale.enlistmentTime
        : 45,
      zones: zones,
      back: false
    }

    data.location = {
      lat: commerce.pointSale.location.lat,
      lng: commerce.pointSale.location.lng,
    }

    if (state.isSpdy) {
      data.dataPhone.isSpdy = state.isSpdy
      sendPost.back = true
    }
    data = processData(data, session)
    delete data.state

    data.createdAt = session.rawExpression('sysdate()')
    data.uuid = session.rawExpression('uuid()')
    data.consecutive = session.rawExpression(
      "sequence('consecutiveService').next()"
    )

    // if (customer) {
    //   data.customer = customer;
    // }
    data.userCreate = user
    // console.log(data,'La data');
    // return res.json(true);
    session.begin()
    delete data.distanceMatrix
    delete data.costDelivery
    let service = await session.insert().into(ClassName).set(data).one()
    // console.log(coverage,'Coverage')

    let queryDataRabbit = `SELECT @rid as rid, name, address, zones:{@rid as rid, name}, location.coordinates as location  FROM ${service.pointSale}`
    let servicePointSale = await session.query(queryDataRabbit).all()

    //send mq
    let noShare = commerce.pointSale.rid.toString()
    noShare = noShare.replace('#', '')
    let channels = [
      `services`,
      `services.created`,
      `services.point-sales.${commerce.pointSale.code}`,
      `services.point-sales.${noShare}`,
    ]

    commit = await session.commit()
    // const serviceRid = service['@rid']
    delete service['@rid']
    delete service['@class']
    delete service['@version']
    service.dataPointSale = servicePointSale[0]
    service.requestdestiny = {
      lat: service.destinationLocation.coordinates[1],
      lng: service.destinationLocation.coordinates[0],
    }

    await RabbitMq.sendMQ(channels, JSON.stringify(service))

    // console.log(commit, "Lo que se envía a rabbit");
    let queryDataRid = `SELECT @rid.asString() as rid FROM Service WHERE consecutive=${service.consecutive}`
    let serviceRid = await session.query(queryDataRid).all()
    // console.log(serviceRid[0].rid,'Query rid')

    let quoteServiceRid = serviceRid[0].rid
    quoteServiceRid = new RID(quoteServiceRid)
    let destinationQuote = {
      lat: data.destinationLocation.coordinates[1],
      lng: data.destinationLocation.coordinates[0],
    }

    try {
      createLog(user, data, quoteServiceRid)
    } catch (error) {
      console.log('Error de log ', error)
    }

    // console.log(quoteServiceRid, "Este rid necesito mirar");
    let quoteData = {
      pointSale: data.pointSale,
      destination: destinationQuote,
      returned: data.returned,
      multiDestiny: data.multiDestiny,
      flag: data.flag,
      service: quoteServiceRid,
    }
    try {
      await this.quote(quoteData, res, null)
    } catch (error) {
      console.log(error)
    }

    if (coverage.zone) {
      sendPost.id = service.consecutive
      sendPost.serviceId = service.rid
      console.log("LINEA DEL sendPost.serviceId");
      console.log(service.rid);

      let t0 = process.hrtime()
      let selfassignDelivery = { data: { brain: true } }
      if (brain) {
        sendPost.serviceId = serviceRid[0].rid
        if (!onlyBrain) {
          console.log('Envia a selfassign ---------------------------------')
          try {
            selfassignDelivery = await selfAssign(sendPost, coverage.rid, res)
            console.log(selfassignDelivery, 'selfassignDelivery')
          } catch (error) {
            console.log(error)
            selfassignDelivery.data.brain = true
          }
        }
        if (selfassignDelivery.data.brain) {
          console.log('Envia a brain ---------------------------------')
          console.log(JSON.stringify({
            sendPost,
            url: wsMatchDeliveryMan.url
          }, null, 2),'Es el objeto Undefined')
          let dataWs = await sendPostData(sendPost, wsMatchDeliveryMan.url)
          console.log(JSON.stringify({ brainResult: dataWs }, null, 2))
          if (dataWs && dataWs.err) {
            ////console.log(dataWs.err);
            return { err: dataWs.err }
            // let dataOwn = sendPostDataOwn(sendPost, res)
            // if (dataOwn && dataOwn.err) {
            //   return { err: dataOwn.err }
            // }
          }
          await session.update(serviceRid[0].rid).set({ integration: dataWs }).one()
        }
      }

      let t1 = process.hrtime(t0)
    }

    session.close()
    // pool.close()
    // client.close()

    return {
      message: res.__('record_insert'),
      uuid: service.uuid,
      consecutive: service.consecutive,
    }
  }